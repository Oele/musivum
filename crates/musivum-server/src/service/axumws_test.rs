// New for musivum: the adapter over a real axum server on the loopback interface, with a
// WebSocket client, under `WsSignalConnection` as the signalling endpoint will use it.

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::Uri;
use axum::routing::get;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use musivum::protocol::livekit::{self, signal_request, signal_response};
use musivum::service::{Error, WsSignalConnection, is_web_socket_close_error};
use musivum_protocol::Message as _;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as ClientMessage;
use tokio_tungstenite::tungstenite::protocol::CloseFrame as ClientCloseFrame;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::*;

const WAIT: Duration = Duration::from_secs(5);

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

// What the endpoint of the test read: a request with its size, or the error that ended it.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum Event {
    Request(livekit::SignalRequest, usize),
    Error(Error),
}

fn param(query: &str, name: &str) -> i64 {
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix(name)?.strip_prefix('='))
        .map_or(0, |v| v.parse().unwrap())
}

// A server whose endpoint at `/ws` answers `ping` with `pong` and reports what it read.
// `limit` is `WsSignalConnection`'s bound, `socket_limit` the socket's (`set_read_limit`);
// with `close` the endpoint closes the connection with a reason after the first request.
async fn start_server() -> (SocketAddr, mpsc::UnboundedReceiver<Event>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let handler = move |ws: WebSocketUpgrade, uri: Uri| {
        let tx = tx.clone();
        async move {
            let query = uri.query().unwrap_or("").to_string();
            let limit = param(&query, "limit");
            let close = query.contains("close");
            set_read_limit(ws, param(&query, "socket_limit")).on_upgrade(move |socket| async move {
                let conn = WsSignalConnection::new(AxumSignalWebSocket::new(socket), limit);
                loop {
                    match conn.read_request().await {
                        Ok((req, size)) => {
                            if let Some(signal_request::Message::Ping(t)) = req.message {
                                let res = livekit::SignalResponse {
                                    message: Some(signal_response::Message::Pong(t)),
                                };
                                conn.write_response(&res).await.unwrap();
                            }
                            let _ = tx.send(Event::Request(req, size));
                            if close {
                                conn.close_with_reason("done").await.unwrap();
                                return;
                            }
                        }
                        Err(err) => {
                            let _ = tx.send(Event::Error(err));
                            let _ = conn.close().await;
                            return;
                        }
                    }
                }
            })
        }
    };
    let app = Router::new().route("/ws", get(handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr, rx)
}

async fn connect(addr: SocketAddr, query: &str) -> Client {
    let (client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws?{query}"))
        .await
        .unwrap();
    client
}

async fn next_event(rx: &mut mpsc::UnboundedReceiver<Event>) -> Event {
    tokio::time::timeout(WAIT, rx.recv())
        .await
        .unwrap()
        .unwrap()
}

async fn next_message(client: &mut Client) -> Option<ClientMessage> {
    tokio::time::timeout(WAIT, client.next())
        .await
        .unwrap()
        .map(|m| m.unwrap())
}

fn ping(t: i64) -> livekit::SignalRequest {
    livekit::SignalRequest {
        message: Some(signal_request::Message::Ping(t)),
    }
}

fn pong(t: i64) -> livekit::SignalResponse {
    livekit::SignalResponse {
        message: Some(signal_response::Message::Pong(t)),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn binary_and_text_requests_are_answered_in_their_form() {
    let (addr, mut events) = start_server().await;
    let mut client = connect(addr, "").await;

    // protobuf in a binary frame: the response comes in one
    let payload = ping(1).encode_to_vec();
    client
        .send(ClientMessage::Binary(Bytes::from(payload.clone())))
        .await
        .unwrap();
    let Event::Request(req, size) = next_event(&mut events).await else {
        panic!("not read");
    };
    assert_eq!(ping(1), req);
    assert_eq!(payload.len(), size);
    let Some(ClientMessage::Binary(data)) = next_message(&mut client).await else {
        panic!("no binary response");
    };
    assert_eq!(
        pong(1),
        livekit::SignalResponse::decode(data.as_ref()).unwrap()
    );

    // JSON in a text frame: the response comes as JSON in one
    let text = serde_json::to_string(&ping(2)).unwrap();
    client
        .send(ClientMessage::Text(text.clone().into()))
        .await
        .unwrap();
    let Event::Request(req, size) = next_event(&mut events).await else {
        panic!("not read");
    };
    assert_eq!(ping(2), req);
    assert_eq!(text.len(), size);
    let Some(ClientMessage::Text(text)) = next_message(&mut client).await else {
        panic!("no text response");
    };
    assert_eq!(
        pong(2),
        serde_json::from_str::<livekit::SignalResponse>(text.as_str()).unwrap()
    );

    // and back to protobuf
    client
        .send(ClientMessage::Binary(Bytes::from(ping(3).encode_to_vec())))
        .await
        .unwrap();
    let Some(ClientMessage::Binary(data)) = next_message(&mut client).await else {
        panic!("no binary response");
    };
    assert_eq!(
        pong(3),
        livekit::SignalResponse::decode(data.as_ref()).unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_message_over_the_limit_is_refused() {
    let (addr, mut events) = start_server().await;

    // WsSignalConnection's bound, with a socket that takes more: a message of exactly the
    // limit is read, one byte more is refused as wsprotocol.rs refuses it
    let mut client = connect(addr, "limit=64").await;
    let at_limit = format!("{:<64}", serde_json::to_string(&ping(4)).unwrap());
    assert_eq!(64, at_limit.len());
    client
        .send(ClientMessage::Text(at_limit.into()))
        .await
        .unwrap();
    let Event::Request(req, 64) = next_event(&mut events).await else {
        panic!("not read");
    };
    assert_eq!(ping(4), req);
    let over = format!("{:<65}", serde_json::to_string(&ping(5)).unwrap());
    client.send(ClientMessage::Text(over.into())).await.unwrap();
    let Event::Error(err) = next_event(&mut events).await else {
        panic!("read");
    };
    assert_eq!(Error::SignalMessageExceedsSizeLimit(64), err);

    // the socket's bound (gorilla's read limit): a close with 1009 and gorilla's error
    let mut client = connect(addr, "limit=64&socket_limit=64").await;
    client
        .send(ClientMessage::Binary(Bytes::from(vec![0u8; 65])))
        .await
        .unwrap();
    let Event::Error(err) = next_event(&mut events).await else {
        panic!("read");
    };
    assert_eq!(
        Error::Io {
            kind: std::io::ErrorKind::InvalidData,
            message: "websocket: read limit exceeded".to_string()
        },
        err
    );
    let Some(ClientMessage::Close(Some(frame))) = next_message(&mut client).await else {
        panic!("not closed");
    };
    assert_eq!(1009, u16::from(frame.code));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_ping_is_answered() {
    let (addr, mut events) = start_server().await;
    let mut client = connect(addr, "").await;

    client
        .send(ClientMessage::Ping(Bytes::from_static(b"abc")))
        .await
        .unwrap();
    let Some(ClientMessage::Pong(data)) = next_message(&mut client).await else {
        panic!("no pong");
    };
    assert_eq!(&b"abc"[..], &data[..]);

    // and the connection goes on
    client
        .send(ClientMessage::Binary(Bytes::from(ping(6).encode_to_vec())))
        .await
        .unwrap();
    let Event::Request(req, _) = next_event(&mut events).await else {
        panic!("not read");
    };
    assert_eq!(ping(6), req);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_close_is_answered_and_ends_the_reading() {
    let (addr, mut events) = start_server().await;

    // the client closes: the server answers with the same code, and the read says so
    let mut client = connect(addr, "").await;
    client
        .send(ClientMessage::Close(Some(ClientCloseFrame {
            code: 1001.into(),
            reason: "bye".into(),
        })))
        .await
        .unwrap();
    let Some(ClientMessage::Close(Some(frame))) = next_message(&mut client).await else {
        panic!("no close");
    };
    assert_eq!(1001, u16::from(frame.code));
    let Event::Error(err) = next_event(&mut events).await else {
        panic!("read");
    };
    assert_eq!(
        Error::WebSocketClose {
            code: 1001,
            text: "bye".to_string()
        },
        err
    );
    assert!(is_web_socket_close_error(&err));

    // the client goes without a close frame: 1006
    let client = connect(addr, "").await;
    drop(client);
    let Event::Error(err) = next_event(&mut events).await else {
        panic!("read");
    };
    assert_eq!(
        Error::WebSocketClose {
            code: 1006,
            text: "unexpected EOF".to_string()
        },
        err
    );

    // the server closes with a reason: a close frame, then the end of the connection
    let mut client = connect(addr, "close").await;
    client
        .send(ClientMessage::Binary(Bytes::from(ping(7).encode_to_vec())))
        .await
        .unwrap();
    let Some(ClientMessage::Binary(_)) = next_message(&mut client).await else {
        panic!("no response");
    };
    let Some(ClientMessage::Close(Some(frame))) = next_message(&mut client).await else {
        panic!("no close");
    };
    assert_eq!(1000, u16::from(frame.code));
    assert_eq!("done", frame.reason.as_str());
    let end = tokio::time::timeout(WAIT, client.next()).await.unwrap();
    assert!(
        !matches!(end, Some(Ok(_))),
        "the connection goes on: {end:?}"
    );
}
