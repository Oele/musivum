// Ported from LiveKit, pkg/service/wsprotocol.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, and made asynchronous. The connection is a
// `SignalWebSocket` (new, below), which a server implements for its WebSocket and a tokio
// task awaits; the original's is `types.WebsocketClient`, gorilla/websocket's blocking
// connection, which needs a goroutine per reader (`rtc::types::WebsocketClient` has that
// shape and is not used here: it would take a thread per connection). gorilla handles the
// control frames inside `NextReader` (a ping is answered with a pong, a pong is dropped, a
// close is answered with a close of the same code and returned as a `*CloseError`, an end of
// the connection without one is a close with 1006); a `SignalWebSocket` hands every frame
// over and `read_message` does that. A frame comes whole, so the size limit is checked on
// it where the original reads through a limited reader. The deadlines of control frames are
// timeouts around the send. `ReadWorkerMessage` and `WriteServerMessage` (agents) are not
// here; `SetReadDeadline` has no counterpart (only agents use it upstream: a server bounds a
// read with a timeout around `read_request`). `NewWSSignalConnection` returns an `Arc`, which
// the ping worker holds until the connection is closed.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use musivum_protocol::Message as _;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::{
    CLOSE_ABNORMAL_CLOSURE, CLOSE_GOING_AWAY, CLOSE_NO_STATUS_RECEIVED, CLOSE_NORMAL_CLOSURE, Error,
};
use crate::go::atomic;
use crate::go::time::{self, Duration};
use crate::protocol::livekit;

const PING_FREQUENCY: Duration = Duration::from_secs(10);
const PING_TIMEOUT: Duration = Duration::from_secs(2);
const CLOSE_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
// gorilla/websocket's writeWait: how long its handlers of control frames take to answer
const WRITE_WAIT: Duration = Duration::from_secs(1);

/// A frame of a WebSocket, as [`SignalWebSocket`] hands it over and sends it. Written new.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WsFrame {
    Text(String),
    Binary(Bytes),
    Ping(Bytes),
    Pong(Bytes),
    /// A close frame with its code and reason; `None` for one without a status (which the
    /// receiver takes for 1005).
    Close(Option<CloseFrame>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseFrame {
    pub code: u16,
    pub reason: String,
}

/// A future a [`SignalWebSocket`] returns.
pub type WsFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What the signalling needs of a WebSocket: the server's own, behind this trait. Written new.
///
/// `next_frame` and `send_frame` may be awaited at the same time from different tasks (one
/// reads while others write: responses, pings); writes may come from several tasks and the
/// implementation sends one frame after another. The socket does no more than carry frames:
/// it hands over pings, pongs and close frames as they come and answers none of them, and
/// it validates frames as RFC 6455 says (masking, UTF-8 of text, close codes). It is
/// expected to bound the size of a message it takes off the wire (`limit.signal_message_size_limit`
/// is the bound the original sets on its socket).
pub trait SignalWebSocket: Send + Sync {
    /// The next frame from the client. `Ok(None)` when the connection ended without a close
    /// frame. An error of the connection is an `io::Error`; kinds `UnexpectedEof`,
    /// `ConnectionReset` and `NotConnected` count as the client going away
    /// ([`is_web_socket_close_error`]).
    fn next_frame(&self) -> WsFuture<'_, io::Result<Option<WsFrame>>>;
    /// Sends a frame.
    fn send_frame(&self, frame: WsFrame) -> WsFuture<'_, io::Result<()>>;
    /// Ends the connection, after the close frame sent before if there was one: Go's
    /// `conn.Close()`.
    fn close(&self) -> WsFuture<'_, io::Result<()>>;
}

// gorilla/websocket's message types: what `read_message` gives on
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MessageType {
    Text,
    Binary,
}

pub struct WsSignalConnection {
    conn: Arc<dyn SignalWebSocket>,
    // serializes the writes, and guards the changes of use_json
    mu: Mutex<()>,
    use_json: atomic::Bool,

    // maximum size (in bytes) of a single decompressed message; 0 disables the limit
    message_size_limit: i64,

    closed: CancellationToken,
}

impl WsSignalConnection {
    pub fn new(conn: Arc<dyn SignalWebSocket>, message_size_limit: i64) -> Arc<WsSignalConnection> {
        let wsc = Arc::new(WsSignalConnection {
            conn,
            mu: Mutex::new(()),
            use_json: atomic::Bool::new(false),
            message_size_limit,
            closed: CancellationToken::new(),
        });
        time::spawn(wsc.clone().ping_worker());
        wsc
    }

    // readMessage reads a single websocket message, bounding the size of the
    // decompressed payload. The transport-level read limit (SetReadLimit) only
    // accounts for the compressed bytes read off the wire, so a small compressed
    // frame can still expand into a much larger buffer once inflated. Reading through
    // an io.LimitReader caps the decompressed size at the same limit.
    async fn read_message(&self) -> Result<(MessageType, Bytes), Error> {
        // what gorilla's NextReader does with the frames before a message
        let (message_type, payload) = loop {
            match self.conn.next_frame().await? {
                Some(WsFrame::Text(text)) => break (MessageType::Text, Bytes::from(text)),
                Some(WsFrame::Binary(data)) => break (MessageType::Binary, data),
                // the default ping handler: answer with a pong; a write that times out is
                // let pass, another failure is the read's
                Some(WsFrame::Ping(data)) => {
                    let pong = self.conn.send_frame(WsFrame::Pong(data));
                    if let Ok(Err(err)) = tokio::time::timeout(WRITE_WAIT.to_std(), pong).await {
                        return Err(err.into());
                    }
                }
                // the default pong handler does nothing
                Some(WsFrame::Pong(_)) => {}
                // the default close handler: answer with the same code, then the read fails
                Some(WsFrame::Close(frame)) => {
                    let (code, text) = match frame {
                        Some(frame) => (frame.code, frame.reason),
                        None => (CLOSE_NO_STATUS_RECEIVED, String::new()),
                    };
                    let message = format_close_message(code, "");
                    let _ = tokio::time::timeout(
                        WRITE_WAIT.to_std(),
                        self.conn.send_frame(WsFrame::Close(message)),
                    )
                    .await;
                    return Err(Error::WebSocketClose { code, text });
                }
                // the connection ended without a close frame: io.ErrUnexpectedEOF
                None => {
                    return Err(Error::WebSocketClose {
                        code: CLOSE_ABNORMAL_CLOSURE,
                        text: "unexpected EOF".to_string(),
                    });
                }
            }
        };

        if self.message_size_limit > 0 {
            // read one byte past the limit so an exactly-at-limit message is accepted
            // while anything larger is detected and rejected
            if payload.len() as i64 > self.message_size_limit {
                return Err(Error::SignalMessageExceedsSizeLimit(
                    self.message_size_limit,
                ));
            }
            return Ok((message_type, payload));
        }

        Ok((message_type, payload))
    }

    pub async fn close(&self) -> Result<(), Error> {
        self.closed.cancel();

        Ok(self.conn.close().await?)
    }

    pub async fn close_with_reason(&self, reason: &str) -> Result<(), Error> {
        self.closed.cancel();

        let msg = format_close_message(CLOSE_NORMAL_CLOSURE, reason);
        let _ = tokio::time::timeout(
            CLOSE_WRITE_TIMEOUT.to_std(),
            self.conn.send_frame(WsFrame::Close(msg)),
        )
        .await;
        Ok(self.conn.close().await?)
    }

    // SetReadDeadline: no counterpart (see the header).

    /// The request, and the size of the message it came in. The original's branch for a
    /// message that is neither text nor binary cannot be taken (gorilla's reader gives no
    /// other), and has no counterpart.
    pub async fn read_request(&self) -> Result<(livekit::SignalRequest, usize), Error> {
        // handle special messages and pass on the rest
        let (message_type, payload) = self.read_message().await?;

        match message_type {
            MessageType::Binary => {
                if self.use_json.load() {
                    let _guard = self.mu.lock().await;
                    // switch to protobuf if client supports it
                    self.use_json.store(false);
                }
                // protobuf encoded
                let msg = livekit::SignalRequest::decode(payload.as_ref())
                    .map_err(|err| Error::ProtoDecode(err.to_string()))?;
                Ok((msg, payload.len()))
            }
            MessageType::Text => {
                {
                    let _guard = self.mu.lock().await;
                    // json encoded, also write back JSON
                    self.use_json.store(true);
                }
                let msg = serde_json::from_slice::<livekit::SignalRequest>(&payload)
                    .map_err(|err| Error::ProtoJson(err.to_string()))?;
                Ok((msg, payload.len()))
            }
        }
    }

    // ReadWorkerMessage: not ported (agents are left out).

    /// The size of the message the response went out in.
    pub async fn write_response(&self, msg: &livekit::SignalResponse) -> Result<usize, Error> {
        let _guard = self.mu.lock().await;

        let (frame, size) = if self.use_json.load() {
            let payload =
                serde_json::to_string(msg).map_err(|err| Error::ProtoJson(err.to_string()))?;
            let size = payload.len();
            (WsFrame::Text(payload), size)
        } else {
            let payload = msg.encode_to_vec();
            let size = payload.len();
            (WsFrame::Binary(Bytes::from(payload)), size)
        };

        self.conn.send_frame(frame).await?;
        Ok(size)
    }

    // WriteServerMessage: not ported (agents are left out).

    async fn ping_worker(self: Arc<Self>) {
        let start = tokio::time::Instant::now() + PING_FREQUENCY.to_std();
        let mut ticker = tokio::time::interval_at(start, PING_FREQUENCY.to_std());
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = self.closed.cancelled() => {
                    return;
                }

                _ = ticker.tick() => {
                    let ping = self.conn.send_frame(WsFrame::Ping(Bytes::new()));
                    match tokio::time::timeout(PING_TIMEOUT.to_std(), ping).await {
                        Ok(Ok(())) => {}
                        _ => return,
                    }
                }
            }
        }
    }
}

// gorilla/websocket's FormatCloseMessage: a close without a status for 1005, which may not be
// sent as a code.
fn format_close_message(close_code: u16, text: &str) -> Option<CloseFrame> {
    if close_code == CLOSE_NO_STATUS_RECEIVED {
        return None;
    }
    Some(CloseFrame {
        code: close_code,
        reason: text.to_string(),
    })
}

// IsWebSocketCloseError checks that error is normal/expected closure
pub fn is_web_socket_close_error(err: &Error) -> bool {
    match err {
        Error::Io { kind, message } => {
            // io.EOF, and Go's texts of a closed or reset connection; the same of a socket
            // that says it with a kind
            *kind == io::ErrorKind::UnexpectedEof
                || *kind == io::ErrorKind::ConnectionReset
                || *kind == io::ErrorKind::NotConnected
                || message.ends_with("use of closed network connection")
                || message.ends_with("connection reset by peer")
        }
        Error::WebSocketClose { code, .. } => matches!(
            *code,
            CLOSE_ABNORMAL_CLOSURE
                | CLOSE_GOING_AWAY
                | CLOSE_NORMAL_CLOSURE
                | CLOSE_NO_STATUS_RECEIVED
        ),
        _ => false,
    }
}

#[cfg(test)]
#[path = "wsprotocol_test.rs"]
mod wsprotocol_test;
