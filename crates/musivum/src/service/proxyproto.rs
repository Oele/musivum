//! What LiveKit's TURN listener (`turn.rs`) takes from go-proxyproto (Apache-2.0): a listener
//! whose connections open with a PROXY protocol header, version 1 (text) or 2 (binary), as
//! HAProxy's specification of the protocol describes them, and whose remote address is then the
//! client's the header names. Written for musivum after that specification, with the behaviour
//! of go-proxyproto v0.15.0's `Listener` and `Conn`:
//!
//! - `Listener::accept` asks the connection policy with the upstream (the proxy's) and the
//!   downstream address; an error that is an untrusted upstream (`InvalidUpstream`, Go's
//!   `ErrInvalidUpstream`) closes the connection and goes on accepting, any other error is
//!   returned; `Skip` gives the connection as it is.
//! - The header is read by the connection's first read or write, within the header timeout
//!   (ten seconds by default); a connection without one under `Require` fails with "proxyproto:
//!   proxy protocol signature not present" (Go's `ErrNoProxyProtocol`), one with one under
//!   `Reject` with `ErrSuperfluousProxyHeader`'s text. What follows the header is read as the
//!   connection's data.
//! - The remote and local addresses are the header's source and destination once it has been
//!   read, unless it is a LOCAL (v2) or UNKNOWN (v1) header; before that they are the
//!   connection's own (Go reads the header when they are asked for).
//!
//! Not here: TLVs (read past), UDP, validators, the buffer size option, and writing headers.

use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

use crate::webrtc::turn::net::{self, Addr, BoxFuture, Conn};

/// go-proxyproto's `DefaultReadHeaderTimeout`.
pub const DEFAULT_READ_HEADER_TIMEOUT: Duration = Duration::from_secs(10);

const SIGV1: &[u8] = b"PROXY";
const SIGV2: &[u8] = b"\r\n\r\n\x00\r\nQUIT\n";

/// go-proxyproto's `Policy`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    // USE address from PROXY header.
    Use,
    // IGNORE address from PROXY header, but accept connection.
    Ignore,
    // REJECT connection when PROXY header is sent
    Reject,
    // REQUIRE connection to send PROXY header, reject if not present
    Require,
    // SKIP accepts a connection without requiring the PROXY header.
    Skip,
}

/// go-proxyproto's `ConnPolicyOptions`.
#[derive(Clone, Copy, Debug)]
pub struct ConnPolicyOptions {
    pub upstream: Addr,
    pub downstream: Addr,
}

/// What a connection policy fails with.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PolicyError {
    /// An error that wraps go-proxyproto's `ErrInvalidUpstream`: the connection is closed and the
    /// listener goes on.
    #[error("proxyproto: upstream connection address not trusted for PROXY information: {0}")]
    InvalidUpstream(String),
    /// Any other error, which `Accept` returns.
    #[error("{0}")]
    Other(String),
}

/// go-proxyproto's `ConnPolicyFunc`.
pub type ConnPolicyFunc =
    Arc<dyn Fn(&ConnPolicyOptions) -> Result<Policy, PolicyError> + Send + Sync>;

/// What a header said: the addresses, or none (LOCAL, UNKNOWN).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Header {
    pub source: Option<SocketAddr>,
    pub destination: Option<SocketAddr>,
}

fn err_no_proxy_protocol() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "proxyproto: proxy protocol signature not present",
    )
}

fn err_invalid(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("proxyproto: {what}"))
}

/// go-proxyproto's `Listener`.
pub struct Listener {
    pub listener: Box<dyn net::Listener>,
    pub conn_policy: Option<ConnPolicyFunc>,
    pub read_header_timeout: Duration,
}

impl net::Listener for Listener {
    fn accept(&self) -> BoxFuture<'_, io::Result<Box<dyn Conn>>> {
        Box::pin(async move {
            loop {
                // Get the underlying connection.
                let conn = self.listener.accept().await?;

                let mut proxy_header_policy = Policy::Require;
                if let Some(conn_policy) = &self.conn_policy {
                    match conn_policy(&ConnPolicyOptions {
                        upstream: conn.remote_addr(),
                        downstream: conn.local_addr(),
                    }) {
                        Ok(policy) => proxy_header_policy = policy,
                        // can't decide the policy, we can't accept the connection.
                        Err(PolicyError::InvalidUpstream(_)) => {
                            drop(conn);
                            // keep listening for other connections.
                            continue;
                        }
                        Err(err) => {
                            drop(conn);
                            return Err(io::Error::other(err));
                        }
                    }
                    // Handle a connection as a regular one.
                    if proxy_header_policy == Policy::Skip {
                        return Ok(conn);
                    }
                }

                let read_header_timeout = if self.read_header_timeout.is_zero() {
                    DEFAULT_READ_HEADER_TIMEOUT
                } else {
                    self.read_header_timeout
                };

                return Ok(Box::new(ProxyConn::new(
                    conn,
                    proxy_header_policy,
                    read_header_timeout,
                )) as Box<dyn Conn>);
            }
        })
    }

    fn close(&self) -> io::Result<()> {
        self.listener.close()
    }

    fn addr(&self) -> Addr {
        self.listener.addr()
    }
}

type HeaderFuture =
    Pin<Box<dyn Future<Output = (Box<dyn Conn>, io::Result<(Option<Header>, Vec<u8>)>)> + Send>>;

enum State {
    Pending(HeaderFuture),
    Ready {
        conn: Box<dyn Conn>,
        buffered: Vec<u8>,
    },
    Failed(io::ErrorKind, String),
}

/// go-proxyproto's `Conn`: a connection whose header is read by its first read or write.
pub struct ProxyConn {
    state: State,
    upstream: Addr,
    downstream: Addr,
    header: Option<Header>,
}

impl ProxyConn {
    pub fn new(conn: Box<dyn Conn>, policy: Policy, read_header_timeout: Duration) -> ProxyConn {
        let (upstream, downstream) = (conn.remote_addr(), conn.local_addr());
        let future = Box::pin(async move {
            let mut conn = conn;
            let read = tokio::time::timeout(read_header_timeout, read_header(&mut conn)).await;
            let result = match read {
                // a header that does not come in time is no header
                Err(_) => Err(err_no_proxy_protocol()),
                Ok(r) => r,
            };
            let result = match (result, policy) {
                (Ok((None, _)), Policy::Require) => Err(err_no_proxy_protocol()),
                (Ok((Some(_), _)), Policy::Reject) => Err(err_invalid(
                    "proxy protocol header present but not expected",
                )),
                (Ok((Some(_), buffered)), Policy::Ignore) => Ok((None, buffered)),
                (r, _) => r,
            };
            (conn, result)
        });
        ProxyConn {
            state: State::Pending(future),
            upstream,
            downstream,
            header: None,
        }
    }

    fn poll_header(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.state {
            State::Pending(future) => match future.as_mut().poll(cx) {
                Poll::Ready((conn, Ok((header, buffered)))) => {
                    self.header = header;
                    self.state = State::Ready { conn, buffered };
                    Poll::Ready(Ok(()))
                }
                Poll::Ready((_, Err(err))) => {
                    self.state = State::Failed(err.kind(), err.to_string());
                    Poll::Ready(Err(err))
                }
                Poll::Pending => Poll::Pending,
            },
            State::Ready { .. } => Poll::Ready(Ok(())),
            State::Failed(kind, msg) => Poll::Ready(Err(io::Error::new(*kind, msg.clone()))),
        }
    }

    /// go-proxyproto's `ProxyHeader`, once it has been read.
    pub fn proxy_header(&self) -> Option<Header> {
        self.header
    }
}

impl Conn for ProxyConn {
    fn local_addr(&self) -> Addr {
        match self.header.and_then(|h| h.destination) {
            Some(a) => Addr::Tcp(a),
            None => self.downstream,
        }
    }

    fn remote_addr(&self) -> Addr {
        match self.header.and_then(|h| h.source) {
            Some(a) => Addr::Tcp(a),
            None => self.upstream,
        }
    }
}

impl AsyncRead for ProxyConn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match this.poll_header(cx) {
            Poll::Ready(Ok(())) => {}
            other => return other,
        }
        let State::Ready { conn, buffered } = &mut this.state else {
            unreachable!("the header has been read");
        };
        if !buffered.is_empty() {
            let n = buffered.len().min(buf.remaining());
            buf.put_slice(&buffered[..n]);
            buffered.drain(..n);
            return Poll::Ready(Ok(()));
        }
        Pin::new(conn.as_mut()).poll_read(cx, buf)
    }
}

impl AsyncWrite for ProxyConn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match this.poll_header(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        }
        let State::Ready { conn, .. } = &mut this.state else {
            unreachable!("the header has been read");
        };
        Pin::new(conn.as_mut()).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().state {
            State::Ready { conn, .. } => Pin::new(conn.as_mut()).poll_flush(cx),
            _ => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().state {
            State::Ready { conn, .. } => Pin::new(conn.as_mut()).poll_shutdown(cx),
            _ => Poll::Ready(Ok(())),
        }
    }
}

// Reads until `buf` has `n` bytes; false if the connection ended first.
async fn fill(conn: &mut Box<dyn Conn>, buf: &mut Vec<u8>, n: usize) -> io::Result<bool> {
    while buf.len() < n {
        let mut b = vec![0u8; n - buf.len()];
        let got = conn.read(&mut b).await?;
        if got == 0 {
            return Ok(false);
        }
        buf.extend_from_slice(&b[..got]);
    }
    Ok(true)
}

/// go-proxyproto's `Read`: the header, if the connection opens with one, and what was read
/// past it (or, without a header, what was read).
pub(crate) async fn read_header(conn: &mut Box<dyn Conn>) -> io::Result<(Option<Header>, Vec<u8>)> {
    let mut buf = Vec::new();
    if !fill(conn, &mut buf, 1).await? {
        return Err(err_no_proxy_protocol());
    }
    if buf[0] != SIGV1[0] && buf[0] != SIGV2[0] {
        return Ok((None, buf));
    }
    if buf[0] == SIGV1[0] {
        if !fill(conn, &mut buf, SIGV1.len()).await? {
            return Err(err_no_proxy_protocol());
        }
        if &buf[..SIGV1.len()] != SIGV1 {
            return Ok((None, buf));
        }
        return parse_version1(conn, buf).await;
    }
    if !fill(conn, &mut buf, SIGV2.len()).await? {
        return Err(err_no_proxy_protocol());
    }
    if &buf[..SIGV2.len()] != SIGV2 {
        return Ok((None, buf));
    }
    parse_version2(conn, buf).await
}

// A v1 line is at most 107 bytes, CRLF included.
const V1_MAX_LENGTH: usize = 107;

async fn parse_version1(
    conn: &mut Box<dyn Conn>,
    mut buf: Vec<u8>,
) -> io::Result<(Option<Header>, Vec<u8>)> {
    let end = loop {
        if let Some(i) = buf.windows(2).position(|w| w == b"\r\n") {
            break i;
        }
        if buf.len() >= V1_MAX_LENGTH {
            return Err(err_invalid("version 1 header line too long"));
        }
        let len = buf.len() + 1;
        if !fill(conn, &mut buf, len).await? {
            return Err(err_invalid("cannot read version 1 header"));
        }
    };
    if end + 2 > V1_MAX_LENGTH {
        return Err(err_invalid("version 1 header line too long"));
    }
    let line =
        std::str::from_utf8(&buf[..end]).map_err(|_| err_invalid("invalid version 1 header"))?;
    let rest = buf[end + 2..].to_vec();
    let tokens: Vec<&str> = line.split(' ').collect();
    if tokens.len() < 2 || tokens[0] != "PROXY" {
        return Err(err_invalid("invalid version 1 header"));
    }
    match tokens[1] {
        "UNKNOWN" => Ok((Some(Header::default()), rest)),
        family @ ("TCP4" | "TCP6") => {
            if tokens.len() != 6 {
                return Err(err_invalid("invalid version 1 header"));
            }
            let ip = |s: &str| -> io::Result<IpAddr> {
                let ip: IpAddr = s.parse().map_err(|_| err_invalid("invalid address"))?;
                match (family, ip) {
                    ("TCP4", IpAddr::V4(_)) | ("TCP6", IpAddr::V6(_)) => Ok(ip),
                    _ => Err(err_invalid("invalid address")),
                }
            };
            let port = |s: &str| -> io::Result<u16> {
                if s.is_empty()
                    || (s.len() > 1 && s.starts_with('0'))
                    || !s.bytes().all(|b| b.is_ascii_digit())
                {
                    return Err(err_invalid("invalid port"));
                }
                s.parse().map_err(|_| err_invalid("invalid port"))
            };
            let source = SocketAddr::new(ip(tokens[2])?, port(tokens[4])?);
            let destination = SocketAddr::new(ip(tokens[3])?, port(tokens[5])?);
            Ok((
                Some(Header {
                    source: Some(source),
                    destination: Some(destination),
                }),
                rest,
            ))
        }
        _ => Err(err_invalid("unsupported protocol in version 1 header")),
    }
}

async fn parse_version2(
    conn: &mut Box<dyn Conn>,
    mut buf: Vec<u8>,
) -> io::Result<(Option<Header>, Vec<u8>)> {
    if !fill(conn, &mut buf, 16).await? {
        return Err(err_invalid("cannot read version 2 header"));
    }
    let ver_cmd = buf[12];
    if ver_cmd >> 4 != 2 {
        return Err(err_invalid("unsupported version 2 header version"));
    }
    let command = ver_cmd & 0x0f;
    let family = buf[13];
    let length = u16::from_be_bytes([buf[14], buf[15]]) as usize;
    if !fill(conn, &mut buf, 16 + length).await? {
        return Err(err_invalid("cannot read version 2 header"));
    }
    let body = &buf[16..16 + length];
    let rest = buf[16 + length..].to_vec();
    match command {
        // LOCAL: the connection is the proxy's own
        0x0 => Ok((Some(Header::default()), rest)),
        0x1 => {
            let header = match family >> 4 {
                // AF_INET
                0x1 => {
                    if body.len() < 12 {
                        return Err(err_invalid("invalid version 2 header length"));
                    }
                    let src = Ipv4Addr::new(body[0], body[1], body[2], body[3]);
                    let dst = Ipv4Addr::new(body[4], body[5], body[6], body[7]);
                    Header {
                        source: Some(SocketAddr::new(
                            src.into(),
                            u16::from_be_bytes([body[8], body[9]]),
                        )),
                        destination: Some(SocketAddr::new(
                            dst.into(),
                            u16::from_be_bytes([body[10], body[11]]),
                        )),
                    }
                }
                // AF_INET6
                0x2 => {
                    if body.len() < 36 {
                        return Err(err_invalid("invalid version 2 header length"));
                    }
                    let octets = |b: &[u8]| -> [u8; 16] { b.try_into().expect("sixteen bytes") };
                    let src = Ipv6Addr::from(octets(&body[0..16]));
                    let dst = Ipv6Addr::from(octets(&body[16..32]));
                    Header {
                        source: Some(SocketAddr::new(
                            src.into(),
                            u16::from_be_bytes([body[32], body[33]]),
                        )),
                        destination: Some(SocketAddr::new(
                            dst.into(),
                            u16::from_be_bytes([body[34], body[35]]),
                        )),
                    }
                }
                // AF_UNSPEC, AF_UNIX: no addresses that are of use here
                _ => Header::default(),
            };
            Ok((Some(header), rest))
        }
        _ => Err(err_invalid("unsupported version 2 command")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    // New for musivum: the binary header (version 2) of an IPv6 client, read and passed by.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_version_2_header_names_the_client() {
        let l = net::listen("tcp4", "127.0.0.1:0").unwrap();
        let addr = net::Listener::addr(&l).socket_addr();
        let listener = Listener {
            listener: Box::new(l),
            conn_policy: None,
            read_header_timeout: Duration::from_secs(5),
        };
        let client = tokio::spawn(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            let mut h = SIGV2.to_vec();
            h.extend_from_slice(&[0x21, 0x21, 0, 36 + 3]);
            h.extend_from_slice(&"2001:db8::7".parse::<Ipv6Addr>().unwrap().octets());
            h.extend_from_slice(&"2001:db8::1".parse::<Ipv6Addr>().unwrap().octets());
            h.extend_from_slice(&40123u16.to_be_bytes());
            h.extend_from_slice(&443u16.to_be_bytes());
            h.extend_from_slice(&[0x04, 0, 0]); // a TLV of nothing, passed by
            h.extend_from_slice(b"data");
            s.write_all(&h).await.unwrap();
            s
        });
        let mut conn = net::Listener::accept(&listener).await.unwrap();
        let mut b = [0u8; 4];
        conn.read_exact(&mut b).await.unwrap();
        assert_eq!(b"data", &b);
        assert_eq!("[2001:db8::7]:40123", conn.remote_addr().to_string());
        assert_eq!("[2001:db8::1]:443", conn.local_addr().to_string());
        drop(client.await.unwrap());
    }

    // New for musivum: the text header (version 1) UNKNOWN keeps the connection's address.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unknown_version_1_header_keeps_the_address() {
        let l = net::listen("tcp4", "127.0.0.1:0").unwrap();
        let addr = net::Listener::addr(&l).socket_addr();
        let listener = Listener {
            listener: Box::new(l),
            conn_policy: None,
            read_header_timeout: Duration::from_secs(5),
        };
        let client = tokio::spawn(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            s.write_all(b"PROXY UNKNOWN\r\nx").await.unwrap();
            s
        });
        let mut conn = net::Listener::accept(&listener).await.unwrap();
        let mut b = [0u8; 1];
        conn.read_exact(&mut b).await.unwrap();
        let s = client.await.unwrap();
        assert_eq!(Addr::Tcp(s.local_addr().unwrap()), conn.remote_addr());
    }
}
