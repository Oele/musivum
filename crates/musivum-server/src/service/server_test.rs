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
    let message_bus = routing::new_local_message_bus();
    let router = routing::create_router(
        current_node.clone(),
        routing::new_signal_client(current_node.node_id(), message_bus.clone()).unwrap(),
        routing::new_room_manager_client(message_bus.clone(), conf.room.clone()).unwrap(),
        routing::default_node_stats_config(),
    );
    let state = ServerState {
        key_provider: create_key_provider(&conf).unwrap(),
        webrtc_config: Arc::new(rtc::WebRtcConfig::new(&conf).unwrap()),
        rtc_node: Node::new(NodeConfig::default()).unwrap(),
        config: Arc::new(conf),
        current_node,
        message_bus,
        router,
    };
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
