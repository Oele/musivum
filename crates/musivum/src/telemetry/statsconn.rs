// Ported from LiveKit, pkg/telemetry/statsconn.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Go's `net.Listener`, `net.Conn` and `net.PacketConn`
// are the TURN server's (`webrtc::turn::net`), as what these wrap is the TURN server's
// sockets: `Listener` has a listener and its `accept` gives the stream as a counted `Conn`;
// `Conn` has anything that reads and writes (`AsyncRead`, `AsyncWrite`) and counts in
// `poll_read` and `poll_write`; `PacketConn` counts in `read_from` and `write_to`. The embedded
// value is a public field named after it. A read or write counts when it succeeds (upstream
// also one that moved bytes and failed). `Close` is the drop of the wrapper, which subtracts the
// connection once (upstream every call of `Close` does); `PacketConn`'s `close` closes the
// socket. `RelayAddressGenerator` wraps pion/turn's as ported, which has no `AllocateConn` and
// `AllocateListener` (RFC 6062's TCP relays are not ported): it counts the relay sockets it
// makes. The test at the end is new: the original has none.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::prometheus;
use crate::webrtc::turn::net::{self as turnnet, Addr, BoxFuture};
use crate::webrtc::turn::{self, AllocateListenerConfig};

pub struct Listener {
    pub listener: Box<dyn turnnet::Listener>,
}

pub fn new_listener(l: Box<dyn turnnet::Listener>) -> Listener {
    Listener { listener: l }
}

impl turnnet::Listener for Listener {
    fn accept(&self) -> BoxFuture<'_, io::Result<Box<dyn turnnet::Conn>>> {
        Box::pin(async move {
            let conn = self.listener.accept().await?;

            Ok(Box::new(new_conn(conn, prometheus::Direction::Incoming)) as Box<dyn turnnet::Conn>)
        })
    }

    fn close(&self) -> io::Result<()> {
        self.listener.close()
    }

    fn addr(&self) -> Addr {
        self.listener.addr()
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

impl turnnet::Conn for Conn<Box<dyn turnnet::Conn>> {
    fn local_addr(&self) -> Addr {
        self.conn.local_addr()
    }

    fn remote_addr(&self) -> Addr {
        self.conn.remote_addr()
    }
}

// Close
impl<C> Drop for Conn<C> {
    fn drop(&mut self) {
        prometheus::sub_connection(self.direction);
    }
}

pub struct PacketConn {
    pub packet_conn: Arc<dyn turnnet::PacketConn>,
    direction: prometheus::Direction,
}

pub fn new_packet_conn(
    c: Arc<dyn turnnet::PacketConn>,
    direction: prometheus::Direction,
) -> PacketConn {
    prometheus::add_connection(direction);
    PacketConn {
        packet_conn: c,
        direction,
    }
}

impl turnnet::PacketConn for PacketConn {
    fn read_from<'a>(&'a self, p: &'a mut [u8]) -> BoxFuture<'a, io::Result<(usize, Addr)>> {
        Box::pin(async move {
            let (n, addr) = self.packet_conn.read_from(p).await?;
            if n > 0 {
                prometheus::increment_bytes("", prometheus::Direction::Incoming, n as u64, false);
                prometheus::increment_packets("", prometheus::Direction::Incoming, 1, false);
            }
            Ok((n, addr))
        })
    }

    fn write_to<'a>(&'a self, p: &'a [u8], addr: Addr) -> BoxFuture<'a, io::Result<usize>> {
        Box::pin(async move {
            let n = self.packet_conn.write_to(p, addr).await?;
            if n > 0 {
                prometheus::increment_bytes("", prometheus::Direction::Outgoing, n as u64, false);
                prometheus::increment_packets("", prometheus::Direction::Outgoing, 1, false);
            }
            Ok(n)
        })
    }

    fn close(&self) -> io::Result<()> {
        self.packet_conn.close()
    }

    fn local_addr(&self) -> Addr {
        self.packet_conn.local_addr()
    }
}

// Close
impl Drop for PacketConn {
    fn drop(&mut self) {
        prometheus::sub_connection(self.direction);
    }
}

pub struct RelayAddressGenerator {
    pub relay_address_generator: Arc<dyn turn::RelayAddressGenerator>,
}

pub fn new_relay_address_generator(
    g: Arc<dyn turn::RelayAddressGenerator>,
) -> RelayAddressGenerator {
    RelayAddressGenerator {
        relay_address_generator: g,
    }
}

impl turn::RelayAddressGenerator for RelayAddressGenerator {
    fn validate(&self) -> Result<(), turn::Error> {
        self.relay_address_generator.validate()
    }

    fn allocate_packet_conn(
        &self,
        c: AllocateListenerConfig,
    ) -> Result<(Arc<dyn turnnet::PacketConn>, Addr), turn::Error> {
        let (conn, addr) = self.relay_address_generator.allocate_packet_conn(c)?;

        Ok((
            Arc::new(new_packet_conn(conn, prometheus::Direction::Outgoing)),
            addr,
        ))
    }

    // Not here: AllocateConn and AllocateListener, see the header.
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use turnnet::{Listener as _, PacketConn as _};

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

        let listener = new_listener(Box::new(turnnet::listen("tcp4", "127.0.0.1:0").unwrap()));
        let addr = listener.addr().socket_addr();
        let client = tokio::spawn(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
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
            turnnet::listen_packet("udp4", "127.0.0.1:0").unwrap(),
            prometheus::Direction::Outgoing,
        );
        let b = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        assert_eq!(
            4,
            a.write_to(b"ping", Addr::Udp(b.local_addr().unwrap()))
                .await
                .unwrap()
        );
        b.send_to(b"pong!", a.local_addr().socket_addr())
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
