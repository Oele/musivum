// Ported from pion/turn, internal/proto/stun_conn.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust, on tokio. The connection is a `net::Conn`, read and
// written from two tasks as Go's `net.Conn` is (behind a lock taken for each poll of it, as
// tokio's `split` does); a write writes the whole frame, one writer at a time. `Close` ends a
// read in progress (`net::err_closed`); the connection itself is closed when the last holder
// lets go of it. The deadlines and `Conn()` (for RFC 6062's ConnectionBind, which is not
// ported) are not here. `ReadFrom` copies what fits into the caller's buffer and gives the
// frame's length, as Go's `copy` does; a connection that ends gives `UnexpectedEof` (Go's
// `io.EOF`).

use std::io;
use std::pin::Pin;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::io::ReadBuf;
use tokio_util::sync::CancellationToken;

use crate::webrtc::stun::message::is_stun_message;
use crate::webrtc::turn::net::{Addr, BoxFuture, Conn, PacketConn, err_closed};

use super::ChannelNumber;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("data is not a valid TURN frame, no STUN or ChannelData found")]
    InvalidTurnFrame,
    #[error("data contains incomplete STUN or TURN frame")]
    IncompleteTurnFrame,
}

// STUNConn wraps a net.Conn and implements
// net.PacketConn by being STUN aware and
// packetizing the stream.
pub struct StunConn {
    next_conn: Arc<Mutex<Box<dyn Conn>>>,
    buff: Mutex<Vec<u8>>,
    write_lock: tokio::sync::Mutex<()>,
    closed: CancellationToken,
}

const STUN_HEADER_SIZE: u16 = 20;
const CHANNEL_DATA_PADDING: u16 = 4;
const CHANNEL_DATA_NUMBER_SIZE: usize = 2;
const CHANNEL_DATA_HEADER_SIZE: u16 = 4;

// Given a buffer give the last offset of the TURN frame
// If the buffer isn't a valid STUN or ChannelData packet,
// or the length doesn't match return false.
pub(crate) fn consume_single_turn_frame(b: &[u8]) -> Result<usize, Error> {
    // Too short to determine if ChannelData or STUN
    if b.len() < 9 {
        return Err(Error::IncompleteTurnFrame);
    }

    let mut datagram_size: u16;
    if is_stun_message(b) {
        datagram_size = u16::from_be_bytes([b[2], b[3]]).wrapping_add(STUN_HEADER_SIZE);
    } else if ChannelNumber(u16::from_be_bytes([b[0], b[1]])).valid() {
        datagram_size =
            u16::from_be_bytes([b[CHANNEL_DATA_NUMBER_SIZE], b[CHANNEL_DATA_NUMBER_SIZE + 1]]);
        let padding_overflow =
            datagram_size.wrapping_add(CHANNEL_DATA_PADDING) % CHANNEL_DATA_PADDING;
        if padding_overflow != 0 {
            datagram_size = datagram_size
                .wrapping_add(CHANNEL_DATA_PADDING)
                .wrapping_sub(padding_overflow);
        }

        datagram_size = datagram_size.wrapping_add(CHANNEL_DATA_HEADER_SIZE);
    } else if b.len() < STUN_HEADER_SIZE as usize {
        return Err(Error::IncompleteTurnFrame);
    } else {
        return Err(Error::InvalidTurnFrame);
    }

    if b.len() < datagram_size as usize {
        return Err(Error::IncompleteTurnFrame);
    }

    Ok(datagram_size as usize)
}

impl StunConn {
    // NewSTUNConn creates a STUNConn.
    pub fn new(next_conn: Box<dyn Conn>) -> StunConn {
        StunConn {
            next_conn: Arc::new(Mutex::new(next_conn)),
            buff: Mutex::new(Vec::new()),
            write_lock: tokio::sync::Mutex::new(()),
            closed: CancellationToken::new(),
        }
    }

    /// The address of the other end (Go's `nextConn.RemoteAddr()`).
    pub fn remote_addr(&self) -> Addr {
        self.next_conn.lock().remote_addr()
    }

    async fn read(&self, p: &mut [u8]) -> io::Result<usize> {
        std::future::poll_fn(|cx| {
            let mut conn = self.next_conn.lock();
            let mut buf = ReadBuf::new(p);
            match Pin::new(&mut **conn).poll_read(cx, &mut buf) {
                std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(Ok(buf.filled().len())),
                std::task::Poll::Ready(Err(err)) => std::task::Poll::Ready(Err(err)),
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        })
        .await
    }

    async fn write_all(&self, mut p: &[u8]) -> io::Result<()> {
        while !p.is_empty() {
            let n = std::future::poll_fn(|cx| {
                let mut conn = self.next_conn.lock();
                Pin::new(&mut **conn).poll_write(cx, p)
            })
            .await?;
            if n == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            p = &p[n..];
        }
        std::future::poll_fn(|cx| {
            let mut conn = self.next_conn.lock();
            Pin::new(&mut **conn).poll_flush(cx)
        })
        .await
    }

    // ReadFrom implements ReadFrom from net.PacketConn.
    pub async fn read_from(&self, payload: &mut [u8]) -> io::Result<(usize, Addr)> {
        loop {
            // First pass any buffered data from previous reads
            {
                let mut buff = self.buff.lock();
                match consume_single_turn_frame(&buff) {
                    Err(Error::InvalidTurnFrame) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            Error::InvalidTurnFrame,
                        ));
                    }
                    Ok(n) => {
                        let m = n.min(payload.len());
                        payload[..m].copy_from_slice(&buff[..m]);
                        buff.drain(..n);
                        drop(buff);

                        return Ok((n, self.remote_addr()));
                    }
                    Err(Error::IncompleteTurnFrame) => {}
                }
            }

            // Then read from the nextConn, appending to our buff
            if self.closed.is_cancelled() {
                return Err(err_closed());
            }
            let n = tokio::select! {
                _ = self.closed.cancelled() => return Err(err_closed()),
                r = self.read(payload) => r?,
            };
            if n == 0 && !payload.is_empty() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
            }

            self.buff.lock().extend_from_slice(&payload[..n]);
        }
    }

    // WriteTo implements WriteTo from net.PacketConn.
    pub async fn write_to(&self, payload: &[u8], _addr: Addr) -> io::Result<usize> {
        if self.closed.is_cancelled() {
            return Err(err_closed());
        }
        let _guard = self.write_lock.lock().await;
        self.write_all(payload).await?;
        Ok(payload.len())
    }

    // Close implements Close from net.PacketConn.
    pub fn close(&self) -> io::Result<()> {
        if self.closed.is_cancelled() {
            return Err(err_closed());
        }
        self.closed.cancel();
        Ok(())
    }

    // LocalAddr implements LocalAddr from net.PacketConn.
    pub fn local_addr(&self) -> Addr {
        self.next_conn.lock().local_addr()
    }
}

impl PacketConn for StunConn {
    fn read_from<'a>(&'a self, p: &'a mut [u8]) -> BoxFuture<'a, io::Result<(usize, Addr)>> {
        Box::pin(StunConn::read_from(self, p))
    }

    fn write_to<'a>(&'a self, p: &'a [u8], addr: Addr) -> BoxFuture<'a, io::Result<usize>> {
        Box::pin(StunConn::write_to(self, p, addr))
    }

    fn close(&self) -> io::Result<()> {
        StunConn::close(self)
    }

    fn local_addr(&self) -> Addr {
        StunConn::local_addr(self)
    }
}

#[cfg(test)]
#[path = "stun_conn_test.rs"]
mod stun_conn_test;
