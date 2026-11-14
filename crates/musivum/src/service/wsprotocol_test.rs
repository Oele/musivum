// Ported from LiveKit, pkg/service/wsprotocol_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The fake WebSocket client of the original, whose
// `NextReader` returns a message, is an in-memory socket (`pair` below) that the test writes
// the message into. The tests after the first are new; the JSON in them is what the original
// writes for the same messages.

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};

use super::*;
use crate::protocol::livekit;
use crate::protocol::livekit::{signal_request, signal_response};
use crate::service::CLOSE_PROTOCOL_ERROR;

// What the server side did, as the client sees it.
#[derive(Debug, PartialEq)]
enum ToClient {
    Frame(WsFrame),
    Closed,
}

struct MemSocket {
    // None: the connection ends without a close frame
    from_client: Mutex<mpsc::UnboundedReceiver<Option<WsFrame>>>,
    to_client: mpsc::UnboundedSender<ToClient>,
}

impl SignalWebSocket for MemSocket {
    fn next_frame(&self) -> WsFuture<'_, io::Result<Option<WsFrame>>> {
        Box::pin(async move {
            let mut rx = self.from_client.lock().await;
            Ok(rx.recv().await.flatten())
        })
    }

    fn send_frame(&self, frame: WsFrame) -> WsFuture<'_, io::Result<()>> {
        Box::pin(async move {
            self.to_client
                .send(ToClient::Frame(frame))
                .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))
        })
    }

    fn close(&self) -> WsFuture<'_, io::Result<()>> {
        Box::pin(async move {
            let _ = self.to_client.send(ToClient::Closed);
            Ok(())
        })
    }
}

struct Client {
    tx: mpsc::UnboundedSender<Option<WsFrame>>,
    rx: mpsc::UnboundedReceiver<ToClient>,
}

impl Client {
    fn send(&self, frame: WsFrame) {
        self.tx.send(Some(frame)).unwrap();
    }

    async fn recv(&mut self) -> ToClient {
        self.rx.recv().await.unwrap()
    }
}

fn pair() -> (Arc<dyn SignalWebSocket>, Client) {
    let (tx, from_client) = mpsc::unbounded_channel();
    let (to_client, rx) = mpsc::unbounded_channel();
    let socket = MemSocket {
        from_client: Mutex::new(from_client),
        to_client,
    };
    (Arc::new(socket), Client { tx, rx })
}

// TestWSSignalConnectionMessageSizeLimit exercises the decompressed-size bound in
// WSSignalConnection. NextReader returns the (already decompressed) message
// stream, so a reader larger than the limit stands in for a small compressed
// frame that expands past the limit once inflated.
#[tokio::test(flavor = "multi_thread")]
async fn test_ws_signal_connection_message_size_limit() {
    const LIMIT: usize = 1024;

    {
        // rejects message larger than limit
        let (fake, client) = pair();
        client.send(WsFrame::Binary(Bytes::from(vec![0u8; LIMIT + 1])));

        let c = WsSignalConnection::new(fake, LIMIT as i64);
        let err = c.read_request().await.unwrap_err();
        assert!(err.to_string().contains("exceeds size limit"));
    }

    {
        // accepts message within limit
        let payload = livekit::SignalRequest::default().encode_to_vec();
        assert!(payload.len() <= LIMIT);

        let (fake, client) = pair();
        client.send(WsFrame::Binary(Bytes::from(payload)));

        let c = WsSignalConnection::new(fake, LIMIT as i64);
        let res = c.read_request().await;
        assert!(res.is_ok());
    }

    {
        // limit of zero reads unbounded payload
        // a payload well beyond any typical limit is read in full when disabled
        let payload = livekit::SignalRequest::default().encode_to_vec();

        let (fake, client) = pair();
        client.send(WsFrame::Binary(Bytes::from(payload)));

        let c = WsSignalConnection::new(fake, 0);
        let res = c.read_request().await;
        assert!(res.is_ok());
    }
}

// What the original writes for these two messages (protojson; its spacing varies).
const PING_REQ_JSON: &str = r#"{"pingReq":{"timestamp":"1700000000123", "rtt":"25"}}"#;
const TRICKLE_JSON: &str = r#"{"trickle":{"candidateInit":"{\"candidate\":\"candidate:1 1 udp 2130706431 192.0.2.1 50000 typ host\",\"sdpMid\":\"0\"}", "target":"SUBSCRIBER", "final":true}}"#;

fn ping_req() -> livekit::SignalRequest {
    livekit::SignalRequest {
        message: Some(signal_request::Message::PingReq(livekit::Ping {
            timestamp: 1700000000123,
            rtt: 25,
        })),
    }
}

fn trickle() -> livekit::SignalResponse {
    livekit::SignalResponse {
        message: Some(signal_response::Message::Trickle(livekit::TrickleRequest {
            candidate_init: r#"{"candidate":"candidate:1 1 udp 2130706431 192.0.2.1 50000 typ host","sdpMid":"0"}"#.into(),
            target: livekit::SignalTarget::Subscriber as i32,
            r#final: true,
        })),
    }
}

fn json(text: &str) -> serde_json::Value {
    serde_json::from_str(text).unwrap()
}

// New for musivum: a request in each form, and the response in the form the client used last.
#[tokio::test(flavor = "multi_thread")]
async fn test_ws_signal_connection_json_and_protobuf() {
    let (socket, mut client) = pair();
    let c = WsSignalConnection::new(socket, 1024);

    // binary: protobuf, and protobuf back
    let payload = ping_req().encode_to_vec();
    client.send(WsFrame::Binary(Bytes::from(payload.clone())));
    let (req, size) = c.read_request().await.unwrap();
    assert_eq!(ping_req(), req);
    assert_eq!(payload.len(), size);

    let size = c.write_response(&trickle()).await.unwrap();
    let ToClient::Frame(WsFrame::Binary(data)) = client.recv().await else {
        panic!("not a binary frame");
    };
    assert_eq!(data.len(), size);
    assert_eq!(trickle(), livekit::SignalResponse::decode(data).unwrap());

    // text: the original's JSON, and JSON back as the original writes it
    client.send(WsFrame::Text(PING_REQ_JSON.to_string()));
    let (req, size) = c.read_request().await.unwrap();
    assert_eq!(ping_req(), req);
    assert_eq!(PING_REQ_JSON.len(), size);
    assert_eq!(
        json(PING_REQ_JSON),
        serde_json::to_value(ping_req()).unwrap()
    );

    let size = c.write_response(&trickle()).await.unwrap();
    let ToClient::Frame(WsFrame::Text(text)) = client.recv().await else {
        panic!("not a text frame");
    };
    assert_eq!(text.len(), size);
    assert_eq!(json(TRICKLE_JSON), json(&text));

    // binary again: the client supports protobuf
    client.send(WsFrame::Binary(Bytes::from(payload)));
    c.read_request().await.unwrap();
    c.write_response(&trickle()).await.unwrap();
    assert!(matches!(
        client.recv().await,
        ToClient::Frame(WsFrame::Binary(_))
    ));

    // the original refuses a field it does not know, and takes the name in the .proto file
    client.send(WsFrame::Text(
        r#"{"pingReq":{"timestamp":"5"},"unknownField":1}"#.to_string(),
    ));
    assert!(matches!(c.read_request().await, Err(Error::ProtoJson(_))));
    client.send(WsFrame::Text(
        r#"{"ping_req":{"timestamp":"5"}}"#.to_string(),
    ));
    let (req, _) = c.read_request().await.unwrap();
    assert_eq!(
        Some(signal_request::Message::PingReq(livekit::Ping {
            timestamp: 5,
            rtt: 0
        })),
        req.message
    );

    // a frame that is no message
    client.send(WsFrame::Binary(Bytes::from_static(&[0xff, 0xff, 0xff])));
    assert!(matches!(c.read_request().await, Err(Error::ProtoDecode(_))));
}

// New for musivum: the limit, control frames and the ends of a connection.
#[tokio::test(flavor = "multi_thread")]
async fn test_ws_signal_connection_frames() {
    let (socket, mut client) = pair();
    let c = WsSignalConnection::new(socket, 16);

    // over the limit, in text too; at the limit is taken
    client.send(WsFrame::Text("x".repeat(17)));
    let err = c.read_request().await.unwrap_err();
    assert_eq!(Error::SignalMessageExceedsSizeLimit(16), err);
    assert_eq!(
        "signal message exceeds size limit of 16 bytes",
        err.to_string()
    );
    assert!(!is_web_socket_close_error(&err));
    let payload = ping_req().encode_to_vec();
    assert!(payload.len() <= 16);
    client.send(WsFrame::Binary(Bytes::from(payload)));
    assert!(c.read_request().await.is_ok());

    // a ping is answered with a pong of the same data, a pong is passed over, and the
    // request after them is read
    client.send(WsFrame::Ping(Bytes::from_static(b"hi")));
    client.send(WsFrame::Pong(Bytes::from_static(b"ho")));
    client.send(WsFrame::Binary(Bytes::from(ping_req().encode_to_vec())));
    let (req, _) = c.read_request().await.unwrap();
    assert_eq!(ping_req(), req);
    assert_eq!(
        ToClient::Frame(WsFrame::Pong(Bytes::from_static(b"hi"))),
        client.recv().await
    );

    // a close is answered with the same code and ends the reading, normally
    client.send(WsFrame::Close(Some(CloseFrame {
        code: CLOSE_NORMAL_CLOSURE,
        reason: "bye".into(),
    })));
    let err = c.read_request().await.unwrap_err();
    assert_eq!("websocket: close 1000 (normal): bye", err.to_string());
    assert!(is_web_socket_close_error(&err));
    assert_eq!(
        ToClient::Frame(WsFrame::Close(Some(CloseFrame {
            code: CLOSE_NORMAL_CLOSURE,
            reason: String::new(),
        }))),
        client.recv().await
    );

    // closing with a reason
    c.close_with_reason("done").await.unwrap();
    assert_eq!(
        ToClient::Frame(WsFrame::Close(Some(CloseFrame {
            code: CLOSE_NORMAL_CLOSURE,
            reason: "done".into(),
        }))),
        client.recv().await
    );
    assert_eq!(ToClient::Closed, client.recv().await);

    // other ends of a connection
    let (socket, client) = pair();
    let c = WsSignalConnection::new(socket, 0);
    client.send(WsFrame::Close(None));
    let err = c.read_request().await.unwrap_err();
    assert_eq!("websocket: close 1005 (no status)", err.to_string());
    assert!(is_web_socket_close_error(&err));
    client.tx.send(None).unwrap();
    let err = c.read_request().await.unwrap_err();
    assert_eq!(
        "websocket: close 1006 (abnormal closure): unexpected EOF",
        err.to_string()
    );
    assert!(is_web_socket_close_error(&err));
    c.close().await.unwrap();

    let err = Error::WebSocketClose {
        code: CLOSE_PROTOCOL_ERROR,
        text: String::new(),
    };
    assert!(!is_web_socket_close_error(&err));
    let err = Error::from(io::Error::other(
        "read tcp: use of closed network connection",
    ));
    assert!(is_web_socket_close_error(&err));
    let err = Error::from(io::Error::from(io::ErrorKind::ConnectionReset));
    assert!(is_web_socket_close_error(&err));
    let err = Error::from(io::Error::from(io::ErrorKind::BrokenPipe));
    assert!(!is_web_socket_close_error(&err));
}

// New for musivum: the ping worker pings every ten seconds until the connection is closed.
#[tokio::test(start_paused = true)]
async fn test_ws_signal_connection_ping_worker() {
    let (socket, mut client) = pair();
    let c = WsSignalConnection::new(socket, 0);
    let start = tokio::time::Instant::now();

    assert_eq!(
        ToClient::Frame(WsFrame::Ping(Bytes::new())),
        client.recv().await
    );
    assert_eq!(std::time::Duration::from_secs(10), start.elapsed());
    assert_eq!(
        ToClient::Frame(WsFrame::Ping(Bytes::new())),
        client.recv().await
    );
    assert_eq!(std::time::Duration::from_secs(20), start.elapsed());

    c.close().await.unwrap();
    assert_eq!(ToClient::Closed, client.recv().await);
    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    assert!(client.rx.try_recv().is_err());
}
