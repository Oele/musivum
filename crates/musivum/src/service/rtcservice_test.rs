// Ported from LiveKit, pkg/service/rtcservice_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The write of `TestDrainMessageSource` returns a future.
// The tests after it are new.
//
// The tests marked "New for musivum" run the endpoint as a server would: a request's parts
// (with the grants of `ApiKeyAuthMiddleware` where a token is needed), the local router with
// a signal server of the test's own on its bus in place of the room manager, and an in-memory
// socket (`pair`) for the upgrade. The expected values are those of a run of the original
// over the same requests, with the same router, a signal server whose session handler
// answers as `TestSignalServer` does, and gorilla's socket (a Go test laid over
// `pkg/service` with `go test -overlay`, run with `scripts/go-reference-test.sh`): the
// `StartSession` the original builds (written by `protojson`), the statuses, headers and
// texts it answers with, and what a client sees on the socket.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use http::{HeaderMap, Request};
use tokio::sync::mpsc;

use super::*;
use crate::config::Config;
use crate::protocol::auth::{AccessToken, ClaimGrants, FileBasedKeyProvider, VideoGrant};
use crate::routing::{
    FakeRoomManagerClient, LocalNode, LocalNodeImpl, LocalRouter, MessageChannel, NodeStatsConfig,
    SignalServerImpl, new_local_message_bus, new_signal_client,
};
use crate::service::{
    ApiKeyAuthMiddleware, LocalStore, ObjectStore, RemoteAddr, WsFrame, WsFuture,
    new_room_allocator, with_grants_expiry,
};
use crate::telemetry::FakeTelemetryService;

fn leave_response(action: livekit::leave_request::Action) -> Message {
    livekit::SignalResponse {
        message: Some(signal_response::Message::Leave(livekit::LeaveRequest {
            action: action as i32,
            reason: livekit::DisconnectReason::Migration as i32,
            ..Default::default()
        })),
    }
    .into()
}

fn leave_action(msg: &Message) -> livekit::leave_request::Action {
    match msg {
        Message::SignalResponse(res) => match &res.message {
            Some(signal_response::Message::Leave(leave)) => leave.action(),
            _ => livekit::leave_request::Action::default(),
        },
        _ => livekit::leave_request::Action::default(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drain_message_source() {
    use livekit::leave_request::Action;

    fn collect(msgs: &mut Vec<Message>) -> impl FnMut(Message) -> std::future::Ready<bool> + '_ {
        move |msg| {
            msgs.push(msg);
            std::future::ready(true)
        }
    }

    {
        // drains queued messages of a closed source
        let source = MessageChannel::new_default("CO_test".into());
        assert_eq!(Ok(()), source.write_message(leave_response(Action::Resume)));
        assert_eq!(
            Ok(()),
            source.write_message(leave_response(Action::Reconnect))
        );
        source.close();

        let mut got = Vec::new();
        assert!(drain_message_source(source.as_ref(), time::SECOND, collect(&mut got)).await);
        assert_eq!(2, got.len());
        assert_eq!(Action::Resume, leave_action(&got[0]));
        assert_eq!(Action::Reconnect, leave_action(&got[1]));
    }

    {
        // drains messages written while draining
        let source = MessageChannel::new_default("CO_test".into());
        // mimics the relay pushing a message that was still in flight when the
        // request direction went away, and closing the source right after
        let s = source.clone();
        tokio::spawn(async move {
            time::sleep(20 * time::MILLISECOND).await;
            let _ = s.write_message(leave_response(Action::Resume));
            s.close();
        });

        let mut got = Vec::new();
        assert!(drain_message_source(source.as_ref(), time::SECOND, collect(&mut got)).await);
        assert_eq!(1, got.len());
    }

    {
        // gives up on the deadline when the source stays open
        let source = MessageChannel::new_default("CO_test".into());
        assert_eq!(Ok(()), source.write_message(leave_response(Action::Resume)));

        let mut got = Vec::new();
        let start = time::now();
        assert!(
            !drain_message_source(source.as_ref(), 50 * time::MILLISECOND, collect(&mut got)).await
        );
        assert!(time::since(start) >= 50 * time::MILLISECOND);
        // what was queued is still flushed
        assert_eq!(1, got.len());
    }

    {
        // stops when the write fails
        let source = MessageChannel::new_default("CO_test".into());
        assert_eq!(Ok(()), source.write_message(leave_response(Action::Resume)));
        assert_eq!(
            Ok(()),
            source.write_message(leave_response(Action::Reconnect))
        );
        source.close();

        let mut got = Vec::new();
        assert!(
            !drain_message_source(source.as_ref(), time::SECOND, |msg| {
                got.push(msg);
                std::future::ready(false)
            })
            .await
        );
        assert_eq!(1, got.len());
    }
}

// ---------------------------------------------------------------------------------------
// The endpoint, new for musivum.

const KEY: &str = "APIkey";
const SECRET: &str = "somesecretencodedinbase62extendto32bytes";
const USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36";
const KEY_HEADER: &str = "dGhlIHNhbXBsZSBub25jZQ==";

struct Session {
    ss: livekit::StartSession,
    source: Arc<dyn MessageSource>,
    sink: Arc<dyn MessageSink>,
}

// The room manager's side, as the session handler of the original's run answers: a join (a
// reconnect response for a reconnect) to every session, a failure for the identity "fail".
#[derive(Default)]
struct TestSignalServer {
    sessions: Mutex<Vec<Session>>,
}

impl SignalServerImpl for TestSignalServer {
    fn relay_signal(
        &self,
        ss: livekit::StartSession,
        request_source: Arc<dyn MessageSource>,
        response_sink: Arc<dyn MessageSink>,
    ) -> Result<(), crate::routing::Error> {
        if ss.identity == "fail" {
            return Err(crate::routing::Error::Json("start session failed".into()));
        }
        let response = if ss.reconnect {
            signal_response::Message::Reconnect(livekit::ReconnectResponse::default())
        } else {
            signal_response::Message::Join(livekit::JoinResponse {
                room: Some(livekit::Room {
                    sid: "RM_test".into(),
                    name: ss.create_room.as_ref().unwrap().name.clone(),
                    ..Default::default()
                }),
                participant: Some(livekit::ParticipantInfo {
                    sid: "PA_test".into(),
                    identity: ss.identity.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            })
        };
        response_sink
            .write_message(
                livekit::SignalResponse {
                    message: Some(response),
                }
                .into(),
            )
            .unwrap();
        self.sessions.lock().push(Session {
            ss,
            source: request_source,
            sink: response_sink,
        });
        Ok(())
    }
}

impl TestSignalServer {
    fn count(&self) -> usize {
        self.sessions.lock().len()
    }

    fn last(&self) -> Session {
        let sessions = self.sessions.lock();
        let s = sessions.last().expect("a session was started");
        Session {
            ss: s.ss.clone(),
            source: s.source.clone(),
            sink: s.sink.clone(),
        }
    }
}

// The service as the original's run had it: no room is created automatically, "room1"
// exists.
fn setup() -> (Arc<RtcService>, Arc<TestSignalServer>) {
    let mut conf = Config::new("", true).unwrap();
    conf.room.auto_create = false;
    let conf = Arc::new(conf);

    let node = LocalNodeImpl::new(None).unwrap();
    node.set_node_id(&"ND_test".into());
    let bus = new_local_message_bus();
    let signal_client = new_signal_client(node.node_id(), bus.clone()).unwrap();
    let router = LocalRouter::new(
        node.clone(),
        signal_client,
        Arc::new(FakeRoomManagerClient::default()),
        NodeStatsConfig::default(),
    );
    let server = Arc::new(TestSignalServer::default());
    bus.register_signal_server(&node.node_id(), server.clone());

    let store = LocalStore::new();
    store
        .store_room(
            &livekit::Room {
                name: "room1".into(),
                ..Default::default()
            },
            None,
        )
        .unwrap();
    let ra = new_room_allocator(conf.clone(), store as Arc<dyn ObjectStore>);

    let svc = RtcService::new(conf, ra, router, Arc::new(FakeTelemetryService::default()));
    (svc, server)
}

fn grants(identity: &str) -> ClaimGrants {
    ClaimGrants {
        identity: identity.into(),
        name: "Alice".into(),
        video: Some(VideoGrant {
            room_join: true,
            room: "room1".into(),
            can_update_own_metadata: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    }
}

// A WebSocket upgrade of `path`, from the address and browser of the original's run, with
// the grants the middleware would have put into it.
fn ws_request(path: &str, grants: Option<ClaimGrants>) -> Parts {
    let mut r = Request::get(path)
        .header("User-Agent", USER_AGENT)
        .header("Connection", "keep-alive, Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", KEY_HEADER)
        .body(())
        .unwrap()
        .into_parts()
        .0;
    r.extensions
        .insert(RemoteAddr("192.0.2.1:1234".parse().unwrap()));
    if let Some(grants) = grants {
        with_grants_expiry(
            &mut r.extensions,
            Some(grants),
            KEY,
            Time::unix(1_800_000_000, 0),
        );
    }
    r
}

// The wrapped join requests of the original's run (`refJoinRequest`): a JS client with
// settings, metadata, attributes (one of them empty), a track and an offer, gzip-compressed;
// and the same as a reconnect of PA_old, not compressed.
const JOIN_GZIP: &str = "CAESYR-LCAAAAAAAAP_i4uFgFGIz0jM01TOQEBBi42AUYJRglGLKTVFi42JMFGI0VOLgYknPz0sVYtDi5GJKNhRizs1MNuLjYs1PS0stEmIuszWQYA5iAAQAAP__E10Dt0kAAAA%3D";
const JOIN_NONE_RECONNECT: &str = "ElUKDAgBEgYyLjE1LjAYEBIGCAEQARgBGgJtZCIGCgFhEgExIggKBGdvbmUSACoJCgJjMRIDbWljMg4KBW9mZmVyEgN2PTAYAzgBQAFKBlBBX29sZFIA";

// What the original sent for each request, as `protojson` wrote it, with the connection ID
// set to CO_test.
const START_SESSIONS: &[(&str, &str, &str)] = &[
    (
        "v1-gzip",
        "/rtc/v1?join_request=",
        r#"{"roomName":"room1", "identity":"alice", "connectionId":"CO_test", "autoSubscribe":true, "autoSubscribeDataTrack":false, "client":{"sdk":"JS", "version":"2.15.0", "protocol":16, "os":"Linux", "deviceModel":"Other", "browser":"Chrome", "browserVersion":"130.0.0", "address":"192.0.2.1"}, "name":"Alice", "grantsJson":"{\"identity\":\"alice\",\"name\":\"Alice\",\"video\":{\"roomJoin\":true,\"room\":\"room1\",\"canUpdateOwnMetadata\":true},\"metadata\":\"md\",\"attributes\":{\"a\":\"1\"}}", "adaptiveStream":true, "subscriberAllowPause":true, "createRoom":{"name":"room1"}, "addTrackRequests":[{"cid":"c1", "name":"mic"}], "publisherOffer":{"type":"offer", "sdp":"v=0", "id":3}, "useSinglePeerConnection":true, "tokenExpiresAt":"1800000000", "apiKey":"APIkey"}"#,
    ),
    (
        "v1-none-reconnect",
        "/rtc/v1?join_request=",
        r#"{"roomName":"room1", "identity":"alice", "connectionId":"CO_test", "reconnect":true, "autoSubscribe":true, "autoSubscribeDataTrack":false, "client":{"sdk":"JS", "version":"2.15.0", "protocol":16, "os":"Linux", "deviceModel":"Other", "browser":"Chrome", "browserVersion":"130.0.0", "address":"192.0.2.1"}, "name":"Alice", "grantsJson":"{\"identity\":\"alice\",\"name\":\"Alice\",\"video\":{\"roomJoin\":true,\"room\":\"room1\",\"canUpdateOwnMetadata\":true},\"metadata\":\"md\",\"attributes\":{\"a\":\"1\"}}", "adaptiveStream":true, "participantId":"PA_old", "reconnectReason":"RR_SIGNAL_DISCONNECTED", "subscriberAllowPause":true, "createRoom":{"name":"room1"}, "addTrackRequests":[{"cid":"c1", "name":"mic"}], "publisherOffer":{"type":"offer", "sdp":"v=0", "id":3}, "useSinglePeerConnection":true, "tokenExpiresAt":"1800000000", "apiKey":"APIkey"}"#,
    ),
    (
        "v0",
        // attributes: {"a":"1","gone":""}
        "/rtc?sdk=js&version=2.1.0&protocol=15&auto_subscribe=0&adaptive_stream=1&disable_ice_lite=true&subscriber_allow_pause=true&auto_subscribe_data_track=1&reconnect_reason=3&attributes=eyJhIjoiMSIsImdvbmUiOiIifQ==",
        r#"{"roomName":"room1", "identity":"alice", "connectionId":"CO_test", "autoSubscribeDataTrack":true, "client":{"sdk":"JS", "version":"2.1.0", "protocol":15, "os":"Linux", "deviceModel":"Other", "browser":"Chrome", "browserVersion":"130.0.0", "address":"192.0.2.1"}, "name":"Alice", "grantsJson":"{\"identity\":\"alice\",\"name\":\"Alice\",\"video\":{\"roomJoin\":true,\"room\":\"room1\",\"canUpdateOwnMetadata\":true},\"attributes\":{\"a\":\"1\"}}", "adaptiveStream":true, "reconnectReason":"RR_SUBSCRIBER_FAILED", "subscriberAllowPause":true, "disableIceLite":true, "createRoom":{"name":"room1"}, "tokenExpiresAt":"1800000000", "apiKey":"APIkey"}"#,
    ),
    (
        "v0-defaults",
        "/rtc?protocol=16",
        r#"{"roomName":"room1", "identity":"alice", "connectionId":"CO_test", "autoSubscribe":true, "client":{"protocol":16, "os":"Linux", "deviceModel":"Other", "browser":"Chrome", "browserVersion":"130.0.0", "address":"192.0.2.1"}, "name":"Alice", "grantsJson":"{\"identity\":\"alice\",\"name\":\"Alice\",\"video\":{\"roomJoin\":true,\"room\":\"room1\",\"canUpdateOwnMetadata\":true}}", "createRoom":{"name":"room1"}, "tokenExpiresAt":"1800000000", "apiKey":"APIkey"}"#,
    ),
    (
        "v0-reconnect",
        "/rtc?reconnect=1&sid=PA_old&reconnect_reason=2&auto_subscribe=1",
        r#"{"roomName":"room1", "identity":"alice", "connectionId":"CO_test", "reconnect":true, "autoSubscribe":true, "client":{"os":"Linux", "deviceModel":"Other", "browser":"Chrome", "browserVersion":"130.0.0", "address":"192.0.2.1"}, "name":"Alice", "grantsJson":"{\"identity\":\"alice\",\"name\":\"Alice\",\"video\":{\"roomJoin\":true,\"room\":\"room1\",\"canUpdateOwnMetadata\":true}}", "participantId":"PA_old", "reconnectReason":"RR_PUBLISHER_FAILED", "createRoom":{"name":"room1"}, "tokenExpiresAt":"1800000000", "apiKey":"APIkey"}"#,
    ),
];

// New for musivum: a connect, in the `/rtc/v1` form with a join request (compressed or not)
// and in the `/rtc` form with the query, reaches the signal server with the `StartSession`
// the original builds for the same request.
#[tokio::test(flavor = "multi_thread")]
async fn a_connect_reaches_the_signal_server_with_the_start_session_of_the_original() {
    let (svc, server) = setup();
    for (i, (name, path, expected)) in START_SESSIONS.iter().enumerate() {
        let path = match *name {
            "v1-gzip" => format!("{path}{JOIN_GZIP}"),
            "v1-none-reconnect" => format!("{path}{JOIN_NONE_RECONNECT}"),
            _ => path.to_string(),
        };
        let r = ws_request(&path, Some(grants("alice")));
        let upgrade = if path.starts_with("/rtc/v1") {
            svc.v1(&r).await
        } else {
            svc.v0(&r).await
        };
        assert!(upgrade.is_ok(), "{name}: {upgrade:?}");
        assert_eq!(i + 1, server.count(), "{name}");

        let mut ss = server.last().ss;
        assert!(ss.connection_id.starts_with("CO_"), "{name}");
        ss.connection_id = "CO_test".into();
        let expected: serde_json::Value = serde_json::from_str(expected).unwrap();
        assert_eq!(expected, serde_json::to_value(&ss).unwrap(), "{name}");
    }
}

// What the server side did, as the client sees it.
#[derive(Debug, PartialEq)]
enum ToClient {
    Frame(WsFrame),
    Closed,
}

// An upgraded connection in memory. Closing it ends a pending read with the error Go gives
// for a closed connection, as a socket of the network does.
struct MemSocket {
    from_client: tokio::sync::Mutex<mpsc::UnboundedReceiver<WsFrame>>,
    to_client: mpsc::UnboundedSender<ToClient>,
    closed: CancellationToken,
}

fn closed_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        "use of closed network connection",
    )
}

impl SignalWebSocket for MemSocket {
    fn next_frame(&self) -> WsFuture<'_, io::Result<Option<WsFrame>>> {
        Box::pin(async move {
            let mut rx = self.from_client.lock().await;
            tokio::select! {
                _ = self.closed.cancelled() => Err(closed_error()),
                frame = rx.recv() => Ok(frame),
            }
        })
    }

    fn send_frame(&self, frame: WsFrame) -> WsFuture<'_, io::Result<()>> {
        Box::pin(async move {
            if self.closed.is_cancelled() {
                return Err(closed_error());
            }
            self.to_client
                .send(ToClient::Frame(frame))
                .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))
        })
    }

    fn close(&self) -> WsFuture<'_, io::Result<()>> {
        Box::pin(async move {
            if !self.closed.is_cancelled() {
                self.closed.cancel();
                let _ = self.to_client.send(ToClient::Closed);
            }
            Ok(())
        })
    }
}

struct Client {
    tx: mpsc::UnboundedSender<WsFrame>,
    rx: mpsc::UnboundedReceiver<ToClient>,
}

impl Client {
    fn send(&self, frame: WsFrame) {
        self.tx.send(frame).unwrap();
    }

    async fn recv(&mut self) -> ToClient {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.rx.recv())
            .await
            .expect("the server writes")
            .expect("the socket is there")
    }

    async fn recv_response(&mut self) -> livekit::SignalResponse {
        match self.recv().await {
            ToClient::Frame(WsFrame::Binary(data)) => {
                livekit::SignalResponse::decode(data.as_ref()).unwrap()
            }
            other => panic!("not a binary response: {other:?}"),
        }
    }

    async fn recv_text(&mut self) -> String {
        match self.recv().await {
            ToClient::Frame(WsFrame::Text(text)) => text,
            other => panic!("not a text response: {other:?}"),
        }
    }
}

fn pair() -> (Arc<dyn SignalWebSocket>, Client) {
    let (tx, from_client) = mpsc::unbounded_channel();
    let (to_client, rx) = mpsc::unbounded_channel();
    let socket = MemSocket {
        from_client: tokio::sync::Mutex::new(from_client),
        to_client,
        closed: CancellationToken::new(),
    };
    (Arc::new(socket), Client { tx, rx })
}

async fn read(source: &Arc<dyn MessageSource>) -> Option<Message> {
    tokio::time::timeout(std::time::Duration::from_secs(5), source.read_chan().recv())
        .await
        .expect("the source is written to or closed")
}

fn request(msg: signal_request::Message) -> livekit::SignalRequest {
    livekit::SignalRequest { message: Some(msg) }
}

fn response(msg: signal_response::Message) -> Message {
    livekit::SignalResponse { message: Some(msg) }.into()
}

// Connects to `path` and runs the session on an in-memory socket.
async fn connect(svc: &RtcService, path: &str) -> (Client, tokio::task::JoinHandle<()>) {
    let r = ws_request(path, Some(grants("alice")));
    let upgrade = svc.v0(&r).await.expect("the request is upgraded");
    assert_eq!(2 << 20, upgrade.read_limit());
    let (socket, client) = pair();
    (client, tokio::spawn(upgrade.run(socket)))
}

// New for musivum: a session over the socket. The first response comes in protobuf; a
// response written to the sink arrives on the socket in the client's framing (protobuf until
// the client writes JSON, JSON after that, as the original answered its JSON ping); a request
// from the socket arrives at the source; a ping is answered on the socket and passed on too.
// When the client closes, the server answers its close, the session's source is closed and
// the session ends.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_carries_responses_and_requests_and_ends_when_the_client_closes() {
    let (svc, server) = setup();
    let (mut client, run) = connect(&svc, "/rtc").await;
    let session = server.last();

    let first = client.recv_response().await;
    let Some(signal_response::Message::Join(join)) = first.message else {
        panic!("not a join: {first:?}");
    };
    assert_eq!("PA_test", join.participant.unwrap().sid);
    assert_eq!(1, svc.connections.mu.lock().len());

    // a response from the participant, in protobuf
    session
        .sink
        .write_message(response(signal_response::Message::RoomUpdate(
            livekit::RoomUpdate {
                room: Some(livekit::Room {
                    sid: "RM_test".into(),
                    ..Default::default()
                }),
            },
        )))
        .unwrap();
    let update = client.recv_response().await;
    assert!(matches!(
        update.message,
        Some(signal_response::Message::RoomUpdate(_))
    ));

    // a request in protobuf reaches the source
    let leave = request(signal_request::Message::Leave(livekit::LeaveRequest {
        reason: livekit::DisconnectReason::ClientInitiated as i32,
        ..Default::default()
    }));
    client.send(WsFrame::Binary(Bytes::from(leave.encode_to_vec())));
    assert_eq!(Some(Message::from(leave)), read(&session.source).await);

    // a ping in JSON: the pong comes in JSON, the ping reaches the source too
    client.send(WsFrame::Text(r#"{"ping":"123"}"#.into()));
    let pong = client.recv_text().await;
    assert!(pong.starts_with(r#"{"pong":"#), "{pong}");
    assert_eq!(
        Some(Message::from(request(signal_request::Message::Ping(123)))),
        read(&session.source).await
    );

    client.send(WsFrame::Text(
        r#"{"pingReq":{"timestamp":"77","rtt":"5"}}"#.into(),
    ));
    let pong_resp = client.recv_text().await;
    assert!(
        pong_resp.starts_with(r#"{"pongResp":{"lastPingTimestamp":"77""#),
        "{pong_resp}"
    );
    assert!(read(&session.source).await.is_some());

    // now responses come in JSON
    session
        .sink
        .write_message(response(signal_response::Message::Pong(5)))
        .unwrap();
    assert_eq!(r#"{"pong":"5"}"#, client.recv_text().await);

    // the client closes: its close is answered, and the session ends
    client.send(WsFrame::Close(Some(crate::service::CloseFrame {
        code: 1000,
        reason: String::new(),
    })));
    assert_eq!(
        ToClient::Frame(WsFrame::Close(Some(crate::service::CloseFrame {
            code: 1000,
            reason: String::new(),
        }))),
        client.recv().await
    );
    assert_eq!(None, read(&session.source).await);
    tokio::time::timeout(std::time::Duration::from_secs(5), run)
        .await
        .expect("the session ends")
        .unwrap();
    assert!(session.sink.is_closed());
    assert_eq!(0, svc.connections.mu.lock().len());
}

// New for musivum: when the participant closes its side, the socket is closed with a normal
// closure and the session ends; what the participant wrote before is still delivered.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_ends_when_the_participant_closes() {
    let (svc, server) = setup();
    let (mut client, run) = connect(&svc, "/rtc").await;
    let session = server.last();
    client.recv_response().await;

    session
        .sink
        .write_message(leave_response(livekit::leave_request::Action::Disconnect))
        .unwrap();
    session.sink.close();

    let leave = client.recv_response().await;
    assert!(matches!(
        leave.message,
        Some(signal_response::Message::Leave(_))
    ));
    assert_eq!(
        ToClient::Frame(WsFrame::Close(Some(crate::service::CloseFrame {
            code: 1000,
            reason: String::new(),
        }))),
        client.recv().await
    );
    assert_eq!(ToClient::Closed, client.recv().await);
    tokio::time::timeout(std::time::Duration::from_secs(5), run)
        .await
        .expect("the session ends")
        .unwrap();
    assert_eq!(None, read(&session.source).await);
    assert_eq!(0, svc.connections.mu.lock().len());
}

// New for musivum: a reconnect (`reconnect=1` with the participant's `sid`) starts a
// session for that participant; its first response, a reconnect response, is the first
// thing on the socket, and the session carries requests as any other.
#[tokio::test(flavor = "multi_thread")]
async fn a_reconnect_starts_a_session_for_the_participant() {
    let (svc, server) = setup();
    let (mut client, run) = connect(&svc, "/rtc?reconnect=1&sid=PA_old&reconnect_reason=2").await;
    let session = server.last();
    assert!(session.ss.reconnect);
    assert_eq!("PA_old", session.ss.participant_id);
    assert_eq!(
        livekit::ReconnectReason::RrPublisherFailed,
        session.ss.reconnect_reason()
    );

    let first = client.recv_response().await;
    assert!(
        matches!(first.message, Some(signal_response::Message::Reconnect(_))),
        "{first:?}"
    );

    let offer = request(signal_request::Message::Offer(
        livekit::SessionDescription {
            r#type: "offer".into(),
            sdp: "v=0".into(),
            ..Default::default()
        },
    ));
    client.send(WsFrame::Binary(Bytes::from(offer.encode_to_vec())));
    assert_eq!(Some(Message::from(offer)), read(&session.source).await);

    // the connection ends without a close frame
    drop(client);
    tokio::time::timeout(std::time::Duration::from_secs(5), run)
        .await
        .expect("the session ends")
        .unwrap();
    assert_eq!(None, read(&session.source).await);
}

// New for musivum: `/rtc/validate` and `/rtc/v1/validate` behind the middleware, with the
// statuses and texts the original answered for a valid token, none, an invalid one, a room
// that cannot be joined (it does not exist and is not created), attributes that cannot be
// decoded, metadata the token may not set, and join requests missing or broken.
#[tokio::test(flavor = "multi_thread")]
async fn validate_answers_as_the_original() {
    let (svc, _server) = setup();
    let mw = ApiKeyAuthMiddleware::new(Arc::new(FileBasedKeyProvider::new_from_map(
        HashMap::from([(KEY.to_string(), SECRET.to_string())]),
    )));
    let token = |room: &str, metadata: bool| {
        AccessToken::new(KEY, SECRET)
            .set_identity("alice")
            .set_video_grant(VideoGrant {
                room_join: true,
                room: room.into(),
                can_update_own_metadata: metadata.then_some(true),
                ..Default::default()
            })
            .to_jwt()
            .unwrap()
    };
    let join = format!("join_request={JOIN_GZIP}");

    let cases: Vec<(&str, String, u16, &str)> = vec![
        (
            "v0-valid",
            format!("/rtc/validate?access_token={}", token("room1", false)),
            200,
            "success",
        ),
        (
            "v0-missing",
            "/rtc/validate".into(),
            401,
            "no permissions to access the room",
        ),
        (
            "v0-invalid",
            "/rtc/validate?access_token=not-a-token".into(),
            401,
            "invalid authorization token",
        ),
        (
            "v0-no-room",
            format!("/rtc/validate?access_token={}", token("room2", false)),
            404,
            "requested room does not exist",
        ),
        (
            "v0-bad-attributes",
            format!(
                "/rtc/validate?attributes=%25%25&access_token={}",
                token("room1", false)
            ),
            400,
            "cannot decode attributes",
        ),
        (
            "v1-metadata-not-allowed",
            format!(
                "/rtc/v1/validate?{join}&access_token={}",
                token("room1", false)
            ),
            401,
            "no permissions to access the room",
        ),
        (
            "v1-valid",
            format!(
                "/rtc/v1/validate?{join}&access_token={}",
                token("room1", true)
            ),
            200,
            "success",
        ),
        (
            "v1-no-room",
            format!(
                "/rtc/v1/validate?{join}&access_token={}",
                token("room2", true)
            ),
            404,
            "requested room does not exist",
        ),
        (
            "v1-no-join-request",
            format!("/rtc/v1/validate?access_token={}", token("room1", false)),
            400,
            "join_request is required",
        ),
        (
            "v1-bad-base64",
            format!(
                "/rtc/v1/validate?join_request=abc&access_token={}",
                token("room1", false)
            ),
            400,
            "cannot base64 decode wrapped join request",
        ),
        (
            "v1-bad-wrapped",
            format!(
                "/rtc/v1/validate?join_request=_w%3D%3D&access_token={}",
                token("room1", false)
            ),
            400,
            "cannot unmarshal wrapped join request",
        ),
        (
            "v1-missing",
            "/rtc/v1/validate".into(),
            401,
            "no permissions to access the room",
        ),
    ];
    for (name, path, status, text) in cases {
        let mut r = Request::get(path.as_str()).body(()).unwrap().into_parts().0;
        let mut w = HeaderMap::new();
        let (got_status, got_text) = match mw.serve_http(&mut w, &mut r) {
            Err(e) => (e.status.as_u16(), e.body),
            Ok(()) => {
                let res = if path.starts_with("/rtc/v1") {
                    svc.v1_validate(&r)
                } else {
                    svc.v0_validate(&r)
                };
                // what Go's server sends: the text's type, which it sniffs
                assert_eq!(
                    "text/plain; charset=utf-8",
                    res.headers()[CONTENT_TYPE],
                    "{name}"
                );
                (
                    res.status().as_u16(),
                    String::from_utf8(res.body().to_vec()).unwrap(),
                )
            }
        };
        assert_eq!((status, text), (got_status, got_text.as_str()), "{name}");
        assert_eq!("*", w["Access-Control-Allow-Origin"], "{name}");
    }
}

// New for musivum: what `/rtc` and `/rtc/v1` answer instead of upgrading, as the original
// did: a request that is not an upgrade; a refused validation; a session that cannot be
// started (the signal server fails it at every attempt); and, once the session has started,
// what gorilla refuses an upgrade for, with its answer and the text of `HandleError` after it.
#[tokio::test(flavor = "multi_thread")]
async fn serve_refuses_as_the_original() {
    let (svc, server) = setup();
    let text = |res: &http::Response<Bytes>| String::from_utf8(res.body().to_vec()).unwrap();
    let headers = |res: &http::Response<Bytes>| {
        let mut hs: Vec<String> = res
            .headers()
            .iter()
            .map(|(k, v)| format!("{}={}", k.as_str(), v.to_str().unwrap()))
            .collect();
        hs.sort();
        hs.join(";")
    };

    // not an upgrade
    let mut r = ws_request("/rtc", Some(grants("alice")));
    r.headers.remove("Upgrade");
    let res = svc.v0(&r).await.unwrap_err();
    assert_eq!(
        (404, String::new(), String::new()),
        (res.status().as_u16(), headers(&res), text(&res))
    );

    // no grants
    let res = svc.v0(&ws_request("/rtc", None)).await.unwrap_err();
    assert_eq!(
        (401, "no permissions to access the room".to_string()),
        (res.status().as_u16(), text(&res))
    );

    let res = svc
        .v1(&ws_request("/rtc/v1", Some(grants("alice"))))
        .await
        .unwrap_err();
    assert_eq!(
        (400, "join_request is required".to_string()),
        (res.status().as_u16(), text(&res))
    );
    assert_eq!(0, server.count());

    // the session cannot be started
    let res = svc
        .v0(&ws_request("/rtc", Some(grants("fail"))))
        .await
        .unwrap_err();
    assert_eq!(
        (500, "connection closed by media".to_string()),
        (res.status().as_u16(), text(&res))
    );

    // refused by the upgrader once the session has started; the session is closed
    let gorilla = "content-type=text/plain; charset=utf-8;sec-websocket-version=13;x-content-type-options=nosniff";
    let mut r = ws_request("/rtc", Some(grants("alice")));
    r.headers
        .insert("Sec-WebSocket-Version", http::HeaderValue::from_static("8"));
    let res = svc.v0(&r).await.unwrap_err();
    assert_eq!(
        (
            400,
            gorilla.to_string(),
            "Bad Request\nwebsocket: unsupported version: 13 not found in 'Sec-Websocket-Version' header".to_string()
        ),
        (res.status().as_u16(), headers(&res), text(&res))
    );
    assert_eq!(1, server.count());
    assert_eq!(None, read(&server.last().source).await);

    let mut r = ws_request("/rtc", Some(grants("alice")));
    r.headers
        .insert("Sec-WebSocket-Key", http::HeaderValue::from_static("short"));
    let res = svc.v0(&r).await.unwrap_err();
    assert_eq!(
        (
            400,
            gorilla.to_string(),
            "Bad Request\nwebsocket: not a websocket handshake: 'Sec-WebSocket-Key' header must be Base64 encoded value of 16-byte in length".to_string()
        ),
        (res.status().as_u16(), headers(&res), text(&res))
    );

    let mut r = ws_request("/rtc", Some(grants("alice")));
    r.method = Method::POST;
    let res = svc.v0(&r).await.unwrap_err();
    assert_eq!(
        (
            405,
            gorilla.to_string(),
            "Method Not Allowed\nwebsocket: the client is not using the websocket protocol: request method is not GET".to_string()
        ),
        (res.status().as_u16(), headers(&res), text(&res))
    );
    assert_eq!(3, server.count());
}
