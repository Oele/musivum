// New for musivum: upstream has no test of server.go. The server on the loopback interface,
// on a port of the system's choosing, with a WebRTC node of its own there too.

use std::net::SocketAddr;
use std::time::{Duration as StdDuration, Instant};

use axum::routing::get;
use musivum::protocol::livekit;
use musivum::webrtc::peerconnection::NodeConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::*;

const CONF: &str = "port: 0\nbind_addresses: [127.0.0.1]\nrtc:\n  node_ip: 127.0.0.1\n  tcp_port: 0\nkeys:\n  key1: a-secret-of-at-least-thirty-two-characters\n";

// What `initialize_server` makes, with a node of the WebRTC connections on the loopback
// interface, and with the routes of `new_mux` and `extra` besides.
fn new_server(extra: axum::Router<ServerState>) -> Arc<LivekitServer> {
    let conf = Config::new(CONF, true).unwrap();
    let current_node: Arc<dyn LocalNode> = routing::LocalNodeImpl::new(Some(&conf)).unwrap();
    let webrtc_config = Arc::new(rtc::WebRtcConfig::new(&conf).unwrap());
    let rtc_node = Node::new(NodeConfig::default()).unwrap();
    let state = new_server_state(conf, current_node, webrtc_config, rtc_node).unwrap();
    let mux = extra
        .with_state(state.clone())
        .merge(new_mux(state.clone()));
    LivekitServer::new(state, mux).unwrap()
}

// Starts the server and waits until it runs: its address, and its `start`.
async fn start(s: &Arc<LivekitServer>) -> (SocketAddr, tokio::task::JoinHandle<Result<(), Error>>) {
    let started = {
        let s = s.clone();
        tokio::spawn(async move { s.start().await })
    };
    let deadline = Instant::now() + StdDuration::from_secs(5);
    while !s.is_running() {
        assert!(Instant::now() < deadline, "the server does not start");
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    (s.local_addrs()[0], started)
}

// A request of HTTP/1.1 on a connection of its own: the status and the body.
async fn get_path(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut conn = TcpStream::connect(addr).await.unwrap();
    request(&mut conn, path).await
}

async fn request(conn: &mut TcpStream, path: &str) -> (u16, String) {
    conn.write_all(
        format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let mut response = String::new();
    conn.read_to_string(&mut response).await.unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    (status, body)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_health_check_answers_on_the_root() {
    let s = new_server(axum::Router::new());
    let (addr, started) = start(&s).await;

    assert_eq!((200, "OK".to_string()), get_path(addr, "/").await);
    // the path is normalised before routing
    assert_eq!((200, "OK".to_string()), get_path(addr, "//").await);
    assert_eq!(
        (404, "404 page not found\n".to_string()),
        get_path(addr, "/nothing").await
    );
    // a token that is not one is refused before any route
    assert_eq!(401, get_path(addr, "/?access_token=nothing").await.0);

    // statistics older than four seconds: not ready
    s.current_node.set_stats(&livekit::NodeStats {
        updated_at: 1_700_000_000,
        ..Default::default()
    });
    assert_eq!(
        (
            406,
            "Not Ready\nNode Updated At 2023-11-14 22:13:20 +0000 UTC".to_string()
        ),
        get_path(addr, "/").await
    );
    // the router's worker brings them up to date
    let deadline = Instant::now() + StdDuration::from_secs(5);
    while get_path(addr, "/").await.0 != 200 {
        assert!(
            Instant::now() < deadline,
            "the node's statistics are not updated"
        );
        tokio::time::sleep(StdDuration::from_millis(100)).await;
    }

    s.stop(false).await;
    started.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_signalling_endpoint_is_routed() {
    let s = new_server(axum::Router::new());
    let (addr, started) = start(&s).await;
    let token = musivum::protocol::auth::AccessToken::new(
        "key1",
        "a-secret-of-at-least-thirty-two-characters",
    )
    .set_identity("alice")
    .set_video_grant(musivum::protocol::auth::VideoGrant {
        room_join: true,
        room: "a-room".to_string(),
        ..Default::default()
    })
    .to_jwt()
    .unwrap();

    // validate: with grants, success, and the header the API key middleware adds for it;
    // `/rtc/v1` wants a join request besides
    for (path, status, body) in [
        ("/rtc/validate", 200, "success"),
        ("/rtc/v1/validate", 400, "join_request is required"),
    ] {
        let mut conn = TcpStream::connect(addr).await.unwrap();
        conn.write_all(
            format!(
                "GET {path}?access_token={token} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        let mut response = String::new();
        conn.read_to_string(&mut response).await.unwrap();
        assert!(
            response.starts_with(&format!("HTTP/1.1 {status}")),
            "{path}: {response}"
        );
        assert!(
            response
                .to_ascii_lowercase()
                .contains("access-control-allow-origin: *"),
            "{path}: {response}"
        );
        assert!(
            response.ends_with(&format!("\r\n\r\n{body}")),
            "{path}: {response}"
        );
    }
    // without grants: refused
    assert_eq!(401, get_path(addr, "/rtc/validate").await.0);
    // `/rtc` and `/rtc/v1` take WebSocket requests only
    let path = format!("/rtc?access_token={token}");
    assert_eq!(404, get_path(addr, &path).await.0);
    let path = format!("/rtc/v1?access_token={token}");
    assert_eq!(404, get_path(addr, &path).await.0);

    s.stop(false).await;
    started.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_shutdown_drains_the_requests_in_progress() {
    let slow = axum::Router::new().route(
        "/slow",
        get(|| async {
            tokio::time::sleep(StdDuration::from_millis(500)).await;
            "done"
        }),
    );
    let s = new_server(slow);
    let (addr, started) = start(&s).await;

    let mut conn = TcpStream::connect(addr).await.unwrap();
    let in_progress = tokio::spawn(async move { request(&mut conn, "/slow").await });
    tokio::time::sleep(StdDuration::from_millis(100)).await;

    let t = Instant::now();
    let stopped = {
        let s = s.clone();
        tokio::spawn(async move { s.stop(false).await })
    };
    // the node is draining, and the server takes no new connection
    tokio::time::sleep(StdDuration::from_millis(100)).await;
    assert_eq!(livekit::NodeState::ShuttingDown, s.node().state());
    assert!(TcpStream::connect(addr).await.is_err());

    // the request in progress is answered, and then the server ends
    assert_eq!((200, "done".to_string()), in_progress.await.unwrap());
    stopped.await.unwrap();
    started.await.unwrap().unwrap();
    assert!(t.elapsed() < StdDuration::from_secs(5), "{:?}", t.elapsed());
    assert!(!s.is_running());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_shutdown_waits_five_seconds_at_most() {
    let slow = axum::Router::new().route(
        "/slow",
        get(|| async {
            tokio::time::sleep(StdDuration::from_secs(60)).await;
            "done"
        }),
    );
    let s = new_server(slow);
    let (addr, started) = start(&s).await;

    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.write_all(b"GET /slow HTTP/1.1\r\nHost: test\r\n\r\n")
        .await
        .unwrap();
    tokio::time::sleep(StdDuration::from_millis(100)).await;

    let t = Instant::now();
    s.stop(false).await;
    started.await.unwrap().unwrap();
    let elapsed = t.elapsed();
    assert!(
        elapsed >= StdDuration::from_millis(4900) && elapsed < StdDuration::from_secs(8),
        "{elapsed:?}"
    );
}

// A POST of JSON with a bearer token on a connection of its own: the status and the body.
async fn post_json(addr: SocketAddr, path: &str, token: &str, body: &str) -> (u16, String) {
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.write_all(
        format!(
            "POST {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut response = String::new();
    conn.read_to_string(&mut response).await.unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    (status, body)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_room_api_is_routed_to_the_room_manager() {
    let s = new_server(axum::Router::new());
    let (addr, started) = start(&s).await;
    let token = musivum::protocol::auth::AccessToken::new(
        "key1",
        "a-secret-of-at-least-thirty-two-characters",
    )
    .set_video_grant(musivum::protocol::auth::VideoGrant {
        room_create: true,
        room_list: true,
        room_admin: true,
        room: "a-room".to_string(),
        ..Default::default()
    })
    .to_jwt()
    .unwrap();
    let call = |method: &str| format!("/twirp/livekit.RoomService/{method}");

    // the room is created through the router and the room manager
    let (status, body) = post_json(addr, &call("CreateRoom"), &token, r#"{"name":"a-room"}"#).await;
    assert_eq!(200, status, "{body}");
    let room: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!("a-room", room["name"]);
    assert!(room["sid"].as_str().unwrap().starts_with("RM_"), "{body}");

    let (status, body) = post_json(addr, &call("ListRooms"), &token, "{}").await;
    assert_eq!(200, status, "{body}");
    assert!(body.contains(r#""name":"a-room""#), "{body}");

    // the room's server on the bus is the room manager: it answers for the room
    let (status, body) = post_json(
        addr,
        &call("ListParticipants"),
        &token,
        r#"{"room":"a-room"}"#,
    )
    .await;
    assert_eq!(200, status, "{body}");
    let (status, body) = post_json(
        addr,
        &call("GetParticipant"),
        &token,
        r#"{"room":"a-room","identity":"nobody"}"#,
    )
    .await;
    assert_eq!(404, status, "{body}");
    assert!(body.contains(r#""code":"not_found""#), "{body}");

    let (status, body) = post_json(addr, &call("DeleteRoom"), &token, r#"{"room":"a-room"}"#).await;
    assert_eq!(200, status, "{body}");
    let (status, body) = post_json(addr, &call("ListRooms"), &token, "{}").await;
    assert_eq!(200, status, "{body}");
    assert!(!body.contains("a-room"), "{body}");

    // without a token: Twirp's unauthenticated error
    let (status, body) = post_json(addr, &call("ListRooms"), "not-a-token", "{}").await;
    assert_eq!(401, status, "{body}");

    s.stop(false).await;
    started.await.unwrap().unwrap();
}
