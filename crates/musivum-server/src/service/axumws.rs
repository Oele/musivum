//! axum's WebSocket as musivum's [`SignalWebSocket`]: what the signalling endpoint hands to
//! [`WsSignalConnection::new`](musivum::service::WsSignalConnection::new). Written new.
//!
//! ```ignore
//! async fn rtc(ws: WebSocketUpgrade, State(s): State<ServerState>) -> Response {
//!     let limit = s.config.limit.signal_message_size_limit;
//!     set_read_limit(ws, limit).on_upgrade(move |socket| async move {
//!         let sig_conn = WsSignalConnection::new(AxumSignalWebSocket::new(socket), limit);
//!         // ...
//!     })
//! }
//! ```
//!
//! [`set_read_limit`] is gorilla's `conn.SetReadLimit`, which the original sets before it
//! makes the connection: a message or frame larger than the limit is not taken off the wire.
//! The socket then answers as gorilla does: a close frame with 1009 and the error "websocket:
//! read limit exceeded". Without compression (which axum's WebSocket does not offer, see
//! below) that limit and the one `WsSignalConnection` checks are the same bytes, so this one
//! refuses first, as gorilla's read limit does upstream for a client that does not compress.
//!
//! How the socket meets the trait's contract:
//! - Reading and writing are two halves (`futures_util`'s split); one read and one write may
//!   be under way at a time, from different tasks, and writes wait for each other.
//! - tungstenite under axum makes the answer to a ping (a pong) and to a close (a close)
//!   itself, and sends it when it next reads or flushes; `WsSignalConnection` answers them
//!   too. The client gets one answer: tungstenite replaces its pending pong with the one
//!   written, and refuses a close written after the client's, which is then sent by a flush
//!   of what tungstenite has. Its answer to a close repeats the client's reason where
//!   gorilla's has none.
//! - A connection that ends without a close frame is `Ok(None)`. A violation of the protocol
//!   is answered as gorilla answers it (`handleProtocolError`), with a close frame of 1002 and
//!   an error "websocket: ...", in tungstenite's words; text that is not UTF-8, which gorilla
//!   passes on, is refused by tungstenite and answered with 1007. An error of the connection
//!   is its `io::Error`.
//! - [`SignalWebSocket::close`] is Go's `conn.Close()`: no close frame of its own; the
//!   connection is dropped once neither half is in use.
//!
//! Compression: LiveKit's upgrader enables permessage-deflate (`EnableCompression: true`).
//! axum 0.8's WebSocket is tungstenite 0.29's, which has no permessage-deflate: the server
//! does not take up a client's offer, and the connection runs uncompressed.

use std::io;
use std::sync::Arc;

use axum::extract::ws::{self, Message, WebSocket, WebSocketUpgrade};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use musivum::service::{
    CLOSE_INVALID_FRAME_PAYLOAD_DATA, CLOSE_MESSAGE_TOO_BIG, CLOSE_PROTOCOL_ERROR, CloseFrame,
    SignalWebSocket, WsFrame, WsFuture,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tungstenite::error::ProtocolError;

// gorilla/websocket's writeWait: how long a close frame it sends on an error may take.
const WRITE_WAIT: std::time::Duration = std::time::Duration::from_secs(1);
// gorilla/websocket's maxControlFramePayloadSize
const MAX_CONTROL_FRAME_PAYLOAD_SIZE: usize = 125;

/// gorilla's `conn.SetReadLimit(limit)`, on the upgrade: no message or frame larger than
/// `limit` bytes is taken. 0 or less leaves tungstenite's own bounds (64 MiB a message,
/// 16 MiB a frame).
pub fn set_read_limit(ws: WebSocketUpgrade, limit: i64) -> WebSocketUpgrade {
    if limit <= 0 {
        return ws;
    }
    let limit = limit as usize;
    ws.max_message_size(limit).max_frame_size(limit)
}

/// An upgraded axum WebSocket, as a [`SignalWebSocket`].
pub struct AxumSignalWebSocket {
    reader: Mutex<Option<SplitStream<WebSocket>>>,
    writer: Mutex<Option<SplitSink<WebSocket, Message>>>,
    closed: CancellationToken,
}

impl AxumSignalWebSocket {
    pub fn new(socket: WebSocket) -> Arc<AxumSignalWebSocket> {
        let (writer, reader) = socket.split();
        Arc::new(AxumSignalWebSocket {
            reader: Mutex::new(Some(reader)),
            writer: Mutex::new(Some(writer)),
            closed: CancellationToken::new(),
        })
    }

    async fn read(&self) -> io::Result<Option<WsFrame>> {
        let mut reader = tokio::select! {
            _ = self.closed.cancelled() => return Err(closed_connection()),
            reader = self.reader.lock() => reader,
        };
        let Some(stream) = reader.as_mut() else {
            return Err(closed_connection());
        };
        let item = tokio::select! {
            _ = self.closed.cancelled() => None,
            item = stream.next() => Some(item),
        };
        if self.closed.is_cancelled() {
            // the half goes, so that the connection is dropped with the other one
            *reader = None;
            return Err(closed_connection());
        }
        drop(reader);
        match item.flatten() {
            None => Ok(None),
            Some(Ok(message)) => Ok(Some(from_message(message))),
            Some(Err(err)) => self.read_error(err).await,
        }
    }

    // What gorilla does on the errors of a read that tungstenite has: a violation of the
    // protocol and a message over the read limit are answered with a close frame.
    async fn read_error(&self, err: axum::Error) -> io::Result<Option<WsFrame>> {
        let err = tungstenite_error(err)?;
        let (code, reason, err) = match err {
            tungstenite::Error::Protocol(ProtocolError::ResetWithoutClosingHandshake) => {
                return Ok(None);
            }
            tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => {
                return Err(closed_connection());
            }
            tungstenite::Error::Io(err) => return Err(err),
            // gorilla's ErrReadLimit
            tungstenite::Error::Capacity(_) => (
                CLOSE_MESSAGE_TOO_BIG,
                String::new(),
                "websocket: read limit exceeded".to_string(),
            ),
            // gorilla passes such text on; tungstenite does not
            tungstenite::Error::Utf8(_) => (
                CLOSE_INVALID_FRAME_PAYLOAD_DATA,
                String::new(),
                "websocket: invalid utf8 payload in text frame".to_string(),
            ),
            // gorilla's handleProtocolError: the message is the close frame's reason too, as
            // much of it as a control frame takes
            err => {
                let mut message = err.to_string();
                while message.len() > MAX_CONTROL_FRAME_PAYLOAD_SIZE - 2 {
                    message.pop();
                }
                let err = format!("websocket: {message}");
                (CLOSE_PROTOCOL_ERROR, message, err)
            }
        };
        let close = WsFrame::Close(Some(CloseFrame { code, reason }));
        let _ = tokio::time::timeout(WRITE_WAIT, self.write(close)).await;
        Err(io::Error::new(io::ErrorKind::InvalidData, err))
    }

    async fn write(&self, frame: WsFrame) -> io::Result<()> {
        let mut writer = tokio::select! {
            _ = self.closed.cancelled() => return Err(closed_connection()),
            writer = self.writer.lock() => writer,
        };
        let Some(sink) = writer.as_mut() else {
            return Err(closed_connection());
        };
        let res = tokio::select! {
            _ = self.closed.cancelled() => Err(closed_connection()),
            res = send(sink, frame) => res,
        };
        if self.closed.is_cancelled() {
            *writer = None;
        }
        res
    }

    fn close_halves(&self) {
        self.closed.cancel();
        // A half that a read or a write holds is dropped by it when the token wakes it.
        if let Ok(mut writer) = self.writer.try_lock() {
            *writer = None;
        }
        if let Ok(mut reader) = self.reader.try_lock() {
            *reader = None;
        }
    }
}

impl SignalWebSocket for AxumSignalWebSocket {
    fn next_frame(&self) -> WsFuture<'_, io::Result<Option<WsFrame>>> {
        Box::pin(self.read())
    }

    fn send_frame(&self, frame: WsFrame) -> WsFuture<'_, io::Result<()>> {
        Box::pin(self.write(frame))
    }

    fn close(&self) -> WsFuture<'_, io::Result<()>> {
        Box::pin(async move {
            self.close_halves();
            Ok(())
        })
    }
}

// What Go says of a connection that was closed here.
fn closed_connection() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        "use of closed network connection",
    )
}

async fn send(sink: &mut SplitSink<WebSocket, Message>, frame: WsFrame) -> io::Result<()> {
    let close = matches!(frame, WsFrame::Close(_));
    match sink
        .send(to_message(frame))
        .await
        .map_err(tungstenite_error)
    {
        // The client closed first. tungstenite sends nothing more but the answer it made
        // when it read the client's close frame, and sends that at the next flush.
        Err(Ok(tungstenite::Error::Protocol(ProtocolError::SendAfterClosing))) if close => {
            match sink.flush().await.map_err(tungstenite_error) {
                Ok(()) | Err(Ok(tungstenite::Error::ConnectionClosed)) => Ok(()),
                Err(err) => Err(write_error(err)),
            }
        }
        res => res.map_err(write_error),
    }
}

// The error of tungstenite that axum's error holds.
fn tungstenite_error(err: axum::Error) -> Result<tungstenite::Error, io::Error> {
    match err.into_inner().downcast::<tungstenite::Error>() {
        Ok(err) => Ok(*err),
        Err(err) => Err(io::Error::other(err)),
    }
}

fn write_error(err: Result<tungstenite::Error, io::Error>) -> io::Error {
    match err {
        Ok(tungstenite::Error::Io(err)) => err,
        Ok(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
            closed_connection()
        }
        Ok(err) => io::Error::other(err),
        Err(err) => err,
    }
}

fn from_message(message: Message) -> WsFrame {
    match message {
        Message::Text(text) => WsFrame::Text(text.as_str().to_string()),
        Message::Binary(data) => WsFrame::Binary(data),
        Message::Ping(data) => WsFrame::Ping(data),
        Message::Pong(data) => WsFrame::Pong(data),
        Message::Close(frame) => WsFrame::Close(frame.map(|frame| CloseFrame {
            code: frame.code,
            reason: frame.reason.as_str().to_string(),
        })),
    }
}

fn to_message(frame: WsFrame) -> Message {
    match frame {
        WsFrame::Text(text) => Message::Text(text.into()),
        WsFrame::Binary(data) => Message::Binary(data),
        WsFrame::Ping(data) => Message::Ping(data),
        WsFrame::Pong(data) => Message::Pong(data),
        WsFrame::Close(frame) => Message::Close(frame.map(|frame| ws::CloseFrame {
            code: frame.code,
            reason: frame.reason.into(),
        })),
    }
}

#[cfg(test)]
#[path = "axumws_test.rs"]
mod axumws_test;
