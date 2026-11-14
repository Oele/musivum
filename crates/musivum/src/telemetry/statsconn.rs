// Ported from LiveKit, pkg/telemetry/statsconn.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Go's `net.Listener`, `net.Conn` and `net.PacketConn`
// are tokio's: `Listener` has a `TcpListener` and its `accept` gives the stream as a counted
// `Conn`; `Conn` has anything that reads and writes (`AsyncRead`, `AsyncWrite`) and counts in
// `poll_read` and `poll_write`; `PacketConn` has a `UdpSocket` and counts in `read_from` and
// `write_to`. The embedded connection is a public field named after it. A read or write
// counts when it succeeds (upstream also one that moved bytes and failed). `Close` is the
// drop of the wrapper, which subtracts the connection once (upstream every call of `Close`
// does). `RelayAddressGenerator`, which wraps an interface of pion/turn, is not ported:
// musivum has no TURN server and no pion/turn. The test at the end is new: the original has
// none.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

use super::prometheus;

pub struct Listener {
    pub listener: TcpListener,
}

pub fn new_listener(l: TcpListener) -> Listener {
    Listener { listener: l }
}

impl Listener {
    pub async fn accept(&self) -> io::Result<Conn<TcpStream>> {
        let (conn, _) = self.listener.accept().await?;

        Ok(new_conn(conn, prometheus::Direction::Incoming))
    }
}

pub struct Conn<C> {
    pub conn: C,
    direction: prometheus::Direction,
}

pub fn new_conn<C>(c: C, direction: prometheus::Direction) -> Conn<C> {
    prometheus::add_connection(direction);
    Conn { conn: c, direction }
}

impl<C: AsyncRead + Unpin> AsyncRead for Conn<C> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let res = Pin::new(&mut self.get_mut().conn).poll_read(cx, buf);
        let n = buf.filled().len() - before;
        if let Poll::Ready(Ok(())) = res
            && n > 0
        {
            prometheus::increment_bytes("", prometheus::Direction::Incoming, n as u64, false);
            prometheus::increment_packets("", prometheus::Direction::Incoming, 1, false);
        }
        res
    }
}

impl<C: AsyncWrite + Unpin> AsyncWrite for Conn<C> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let res = Pin::new(&mut self.get_mut().conn).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = res
            && n > 0
        {
            prometheus::increment_bytes("", prometheus::Direction::Outgoing, n as u64, false);
            prometheus::increment_packets("", prometheus::Direction::Outgoing, 1, false);
        }
        res
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().conn).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().conn).poll_shutdown(cx)
    }
}

// Close
impl<C> Drop for Conn<C> {
    fn drop(&mut self) {
        prometheus::sub_connection(self.direction);
    }
}

pub struct PacketConn {
    pub packet_conn: UdpSocket,
    direction: prometheus::Direction,
}

pub fn new_packet_conn(c: UdpSocket, direction: prometheus::Direction) -> PacketConn {
    prometheus::add_connection(direction);
    PacketConn {
        packet_conn: c,
        direction,
    }
}

impl PacketConn {
    pub async fn read_from(&self, p: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (n, addr) = self.packet_conn.recv_from(p).await?;
        if n > 0 {
            prometheus::increment_bytes("", prometheus::Direction::Incoming, n as u64, false);
            prometheus::increment_packets("", prometheus::Direction::Incoming, 1, false);
        }
        Ok((n, addr))
    }

    pub async fn write_to(&self, p: &[u8], addr: SocketAddr) -> io::Result<usize> {
        let n = self.packet_conn.send_to(p, addr).await?;
        if n > 0 {
            prometheus::increment_bytes("", prometheus::Direction::Outgoing, n as u64, false);
            prometheus::increment_packets("", prometheus::Direction::Outgoing, 1, false);
        }
        Ok(n)
    }
}

// Close
impl Drop for PacketConn {
    fn drop(&mut self) {
        prometheus::sub_connection(self.direction);
    }
}

// Not here: RelayAddressGenerator, see the header.

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    fn bytes(direction: prometheus::Direction) -> u64 {
        prometheus::PROM_PACKET_BYTES
            .with_label_values([direction.as_str(), "initial", ""])
            .get()
    }

    // New for musivum.
    #[tokio::test(flavor = "multi_thread")]
    async fn connections_count_what_they_read_and_write() {
        let (incoming, outgoing) = (
            bytes(prometheus::Direction::Incoming),
            bytes(prometheus::Direction::Outgoing),
        );

        let listener = new_listener(TcpListener::bind("127.0.0.1:0").await.unwrap());
        let addr = listener.listener.local_addr().unwrap();
        let client = tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            s.write_all(b"hello").await.unwrap();
            let mut answer = [0u8; 3];
            s.read_exact(&mut answer).await.unwrap();
            answer
        });
        let mut conn = listener.accept().await.unwrap();
        let mut got = [0u8; 5];
        conn.read_exact(&mut got).await.unwrap();
        conn.write_all(b"hey").await.unwrap();
        assert_eq!(b"hey", &client.await.unwrap());
        assert_eq!(b"hello", &got);
        drop(conn);

        let a = new_packet_conn(
            UdpSocket::bind("127.0.0.1:0").await.unwrap(),
            prometheus::Direction::Outgoing,
        );
        let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        assert_eq!(
            4,
            a.write_to(b"ping", b.local_addr().unwrap()).await.unwrap()
        );
        b.send_to(b"pong!", a.packet_conn.local_addr().unwrap())
            .await
            .unwrap();
        let mut buf = [0u8; 16];
        let (n, _) = a.read_from(&mut buf).await.unwrap();
        assert_eq!(b"pong!", &buf[..n]);

        // other tests count bytes of no country too
        assert!(bytes(prometheus::Direction::Incoming) >= incoming + 5 + 5);
        assert!(bytes(prometheus::Direction::Outgoing) >= outgoing + 3 + 4);
    }
}
