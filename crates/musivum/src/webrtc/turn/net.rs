//! What pion/turn takes from Go's `net` and `crypto/tls`, on tokio: the address of a peer
//! (`net.Addr`, as a `*net.UDPAddr` or a `*net.TCPAddr`), a socket that sends and receives
//! datagrams (`net.PacketConn`), a listener (`net.Listener`) and the streams it accepts
//! (`net.Conn`), TCP and TLS ones (`net.Listen`, `tls.NewListener`), and a network of
//! addresses (`net.IPNet`, as `net.ParseCIDR` reads one).
//!
//! A socket is closed by `close`, which ends a read waiting on it with
//! [`err_closed`] (Go's `net.ErrClosed`) at once, as Go's `Close` does; the descriptor itself
//! is let go when the last holder drops it.

use std::fmt;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::sync::CancellationToken;

/// A future a trait object can return.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Go's `net.Addr` as pion/turn meets it: a `*net.UDPAddr` or a `*net.TCPAddr`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Addr {
    Udp(SocketAddr),
    Tcp(SocketAddr),
}

impl Addr {
    /// Go's `Network()`.
    pub fn network(&self) -> &'static str {
        match self {
            Addr::Udp(_) => "udp",
            Addr::Tcp(_) => "tcp",
        }
    }

    pub fn socket_addr(&self) -> SocketAddr {
        match *self {
            Addr::Udp(a) | Addr::Tcp(a) => a,
        }
    }

    pub fn ip(&self) -> IpAddr {
        self.socket_addr().ip()
    }

    pub fn port(&self) -> u16 {
        self.socket_addr().port()
    }
}

impl fmt::Display for Addr {
    // Go's `String()`: an IPv4 address in an IPv6 one is written as the IPv4 address.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let a = self.socket_addr();
        match (a, a.ip().to_canonical()) {
            (SocketAddr::V6(v6), IpAddr::V6(ip)) if v6.scope_id() != 0 => {
                write!(f, "[{}%{}]:{}", ip, v6.scope_id(), a.port())
            }
            (_, ip) => write!(f, "{}", SocketAddr::new(ip, a.port())),
        }
    }
}

impl fmt::Debug for Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// Go's `net.ErrClosed`.
#[derive(Debug)]
struct ErrClosed;

impl fmt::Display for ErrClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("use of closed network connection")
    }
}

impl std::error::Error for ErrClosed {}

/// Go's `net.ErrClosed`, as an I/O error.
pub fn err_closed() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, ErrClosed)
}

/// Go's `errors.Is(err, net.ErrClosed)`.
pub fn is_closed(err: &io::Error) -> bool {
    err.get_ref().is_some_and(|e| e.is::<ErrClosed>())
}

/// Go's `net.PacketConn`.
pub trait PacketConn: Send + Sync {
    fn read_from<'a>(&'a self, p: &'a mut [u8]) -> BoxFuture<'a, io::Result<(usize, Addr)>>;
    fn write_to<'a>(&'a self, p: &'a [u8], addr: Addr) -> BoxFuture<'a, io::Result<usize>>;
    fn close(&self) -> io::Result<()>;
    fn local_addr(&self) -> Addr;
}

/// Go's `net.Conn`: a stream with its two addresses.
pub trait Conn: AsyncRead + AsyncWrite + Send + Unpin {
    fn local_addr(&self) -> Addr;
    fn remote_addr(&self) -> Addr;
    /// Go's `conn.(*tls.Conn)`.
    fn as_tls(&mut self) -> Option<&mut TlsConn> {
        None
    }
}

/// Go's `net.Listener`.
pub trait Listener: Send + Sync {
    fn accept(&self) -> BoxFuture<'_, io::Result<Box<dyn Conn>>>;
    fn close(&self) -> io::Result<()>;
    fn addr(&self) -> Addr;
}

/// Go's `net.SplitHostPort`, for the forms the TURN server is given: `host:port`, `[v6]:port`.
fn split_host_port(address: &str) -> io::Result<(&str, u16)> {
    let bad = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("address {address}: missing port in address"),
        )
    };
    let (host, port) = address.rsplit_once(':').ok_or_else(bad)?;
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    let port = port.parse::<u16>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("address {address}: invalid port"),
        )
    })?;
    Ok((host, port))
}

/// Go's resolution of a listening address for a network (`udp`, `udp4`, `udp6`, `tcp`,
/// `tcp4`, `tcp6`): an empty host is all addresses of the family, a host is an IP address
/// (no names are looked up), of the network's family.
fn resolve_listen_addr(network: &str, address: &str) -> io::Result<SocketAddr> {
    let (host, port) = split_host_port(address)?;
    let family = network.chars().last().filter(|c| *c == '4' || *c == '6');
    let ip: IpAddr = if host.is_empty() {
        match family {
            Some('6') => Ipv6Addr::UNSPECIFIED.into(),
            _ => Ipv4Addr::UNSPECIFIED.into(),
        }
    } else {
        host.parse().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("address {address}: no suitable address found"),
            )
        })?
    };
    let ip = ip.to_canonical();
    match (family, ip) {
        (Some('4'), IpAddr::V6(_)) | (Some('6'), IpAddr::V4(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("address {address}: no suitable address found"),
        )),
        _ => Ok(SocketAddr::new(ip, port)),
    }
}

/// A UDP socket: Go's `*net.UDPConn` as `net.ListenPacket` gives it.
pub struct UdpConn {
    socket: tokio::net::UdpSocket,
    local_addr: SocketAddr,
    closed: CancellationToken,
}

/// Go's `net.ListenPacket(network, address)` for a UDP network. Needs a tokio runtime
/// (`crate::go::time::runtime` is entered when there is none).
pub fn listen_packet(network: &str, address: &str) -> io::Result<Arc<UdpConn>> {
    let addr = resolve_listen_addr(network, address)?;
    let socket = std::net::UdpSocket::bind(addr)?;
    Ok(Arc::new(UdpConn::from_std(socket)?))
}

impl UdpConn {
    pub fn from_std(socket: std::net::UdpSocket) -> io::Result<UdpConn> {
        socket.set_nonblocking(true)?;
        let runtime = crate::go::time::runtime();
        let _guard = runtime.enter();
        let socket = tokio::net::UdpSocket::from_std(socket)?;
        let local_addr = socket.local_addr()?;
        Ok(UdpConn {
            socket,
            local_addr,
            closed: CancellationToken::new(),
        })
    }

    pub fn is_closed(&self) -> bool {
        self.closed.is_cancelled()
    }
}

impl PacketConn for UdpConn {
    fn read_from<'a>(&'a self, p: &'a mut [u8]) -> BoxFuture<'a, io::Result<(usize, Addr)>> {
        Box::pin(async move {
            if self.closed.is_cancelled() {
                return Err(err_closed());
            }
            tokio::select! {
                _ = self.closed.cancelled() => Err(err_closed()),
                r = self.socket.recv_from(p) => {
                    let (n, from) = r?;
                    Ok((n, Addr::Udp(SocketAddr::new(from.ip().to_canonical(), from.port()))))
                }
            }
        })
    }

    fn write_to<'a>(&'a self, p: &'a [u8], addr: Addr) -> BoxFuture<'a, io::Result<usize>> {
        Box::pin(async move {
            if self.closed.is_cancelled() {
                return Err(err_closed());
            }
            let mut to = addr.socket_addr();
            // An IPv4 peer of a socket of IPv6 addresses is reached at its IPv6 form, as Go
            // does by itself.
            if let (SocketAddr::V6(_), IpAddr::V4(ip)) = (self.local_addr, to.ip()) {
                to = SocketAddr::new(IpAddr::V6(ip.to_ipv6_mapped()), to.port());
            }
            self.socket.send_to(p, to).await
        })
    }

    fn close(&self) -> io::Result<()> {
        if self.closed.is_cancelled() {
            return Err(err_closed());
        }
        self.closed.cancel();
        Ok(())
    }

    fn local_addr(&self) -> Addr {
        Addr::Udp(self.local_addr)
    }
}

/// A TCP listener: Go's `*net.TCPListener` as `net.Listen` gives it.
pub struct TcpListener {
    listener: tokio::net::TcpListener,
    local_addr: SocketAddr,
    closed: CancellationToken,
}

/// Go's `net.Listen(network, address)` for a TCP network. Needs a tokio runtime
/// (`crate::go::time::runtime` is entered when there is none).
pub fn listen(network: &str, address: &str) -> io::Result<TcpListener> {
    let addr = resolve_listen_addr(network, address)?;
    let listener = std::net::TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let runtime = crate::go::time::runtime();
    let _guard = runtime.enter();
    let listener = tokio::net::TcpListener::from_std(listener)?;
    let local_addr = listener.local_addr()?;
    Ok(TcpListener {
        listener,
        local_addr,
        closed: CancellationToken::new(),
    })
}

impl Listener for TcpListener {
    fn accept(&self) -> BoxFuture<'_, io::Result<Box<dyn Conn>>> {
        Box::pin(async move {
            if self.closed.is_cancelled() {
                return Err(err_closed());
            }
            tokio::select! {
                _ = self.closed.cancelled() => Err(err_closed()),
                r = self.listener.accept() => {
                    let (stream, remote_addr) = r?;
                    let local_addr = stream.local_addr()?;
                    Ok(Box::new(TcpConn { stream, local_addr, remote_addr }) as Box<dyn Conn>)
                }
            }
        })
    }

    fn close(&self) -> io::Result<()> {
        if self.closed.is_cancelled() {
            return Err(err_closed());
        }
        self.closed.cancel();
        Ok(())
    }

    fn addr(&self) -> Addr {
        Addr::Tcp(self.local_addr)
    }
}

/// A TCP connection: Go's `*net.TCPConn`.
pub struct TcpConn {
    stream: tokio::net::TcpStream,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
}

impl TcpConn {
    pub fn new(stream: tokio::net::TcpStream) -> io::Result<TcpConn> {
        Ok(TcpConn {
            local_addr: stream.local_addr()?,
            remote_addr: stream.peer_addr()?,
            stream,
        })
    }
}

impl Conn for TcpConn {
    fn local_addr(&self) -> Addr {
        Addr::Tcp(self.local_addr)
    }

    fn remote_addr(&self) -> Addr {
        Addr::Tcp(self.remote_addr)
    }
}

impl AsyncRead for TcpConn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpConn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

/// What Go's `tls.ConnectionState` says that a TURN server may want to know.
#[derive(Clone, Debug, Default)]
pub struct ConnectionState {
    pub handshake_complete: bool,
    pub server_name: String,
    pub peer_certificates: Vec<rustls::pki_types::CertificateDer<'static>>,
}

enum TlsState {
    Handshaking(Box<tokio_rustls::Accept<Box<dyn Conn>>>),
    Ready(Box<tokio_rustls::server::TlsStream<Box<dyn Conn>>>),
    Failed,
}

/// A server's TLS connection: Go's `*tls.Conn` as `tls.Server` makes it. As there, the
/// handshake is made by the first read or write, or by `handshake`.
pub struct TlsConn {
    state: TlsState,
    local_addr: Addr,
    remote_addr: Addr,
}

impl TlsConn {
    fn poll_handshake(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.state {
            TlsState::Handshaking(accept) => match Pin::new(accept.as_mut()).poll(cx) {
                Poll::Ready(Ok(stream)) => {
                    // the addresses of a connection that read a PROXY header are known now
                    self.local_addr = stream.get_ref().0.local_addr();
                    self.remote_addr = stream.get_ref().0.remote_addr();
                    self.state = TlsState::Ready(Box::new(stream));
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(err)) => {
                    self.state = TlsState::Failed;
                    Poll::Ready(Err(err))
                }
                Poll::Pending => Poll::Pending,
            },
            TlsState::Ready(_) => Poll::Ready(Ok(())),
            TlsState::Failed => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "tls: the handshake failed",
            ))),
        }
    }

    /// Go's `HandshakeContext` (the deadline is the caller's).
    pub async fn handshake(&mut self) -> io::Result<()> {
        std::future::poll_fn(|cx| self.poll_handshake(cx)).await
    }

    /// Go's `ConnectionState`.
    pub fn connection_state(&self) -> ConnectionState {
        match &self.state {
            TlsState::Ready(stream) => {
                let conn = stream.get_ref().1;
                ConnectionState {
                    handshake_complete: true,
                    server_name: conn.server_name().unwrap_or_default().to_string(),
                    peer_certificates: conn
                        .peer_certificates()
                        .map(|c| c.iter().map(|c| c.clone().into_owned()).collect())
                        .unwrap_or_default(),
                }
            }
            _ => ConnectionState::default(),
        }
    }
}

impl Conn for TlsConn {
    fn local_addr(&self) -> Addr {
        match &self.state {
            TlsState::Handshaking(accept) => {
                accept.get_ref().map_or(self.local_addr, |c| c.local_addr())
            }
            _ => self.local_addr,
        }
    }

    fn remote_addr(&self) -> Addr {
        match &self.state {
            TlsState::Handshaking(accept) => accept
                .get_ref()
                .map_or(self.remote_addr, |c| c.remote_addr()),
            _ => self.remote_addr,
        }
    }

    fn as_tls(&mut self) -> Option<&mut TlsConn> {
        Some(self)
    }
}

impl AsyncRead for TlsConn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match this.poll_handshake(cx) {
            Poll::Ready(Ok(())) => {}
            other => return other,
        }
        match &mut this.state {
            TlsState::Ready(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
            _ => unreachable!("the handshake is done"),
        }
    }
}

impl AsyncWrite for TlsConn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match this.poll_handshake(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        }
        match &mut this.state {
            TlsState::Ready(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
            _ => unreachable!("the handshake is done"),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().state {
            TlsState::Ready(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
            _ => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().state {
            TlsState::Ready(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
            _ => Poll::Ready(Ok(())),
        }
    }
}

/// A listener whose connections are TLS servers: Go's `tls.NewListener`.
pub struct TlsListener {
    inner: Box<dyn Listener>,
    acceptor: tokio_rustls::TlsAcceptor,
}

/// Go's `tls.NewListener(inner, config)`.
pub fn new_tls_listener(
    inner: Box<dyn Listener>,
    config: Arc<rustls::ServerConfig>,
) -> TlsListener {
    TlsListener {
        inner,
        acceptor: tokio_rustls::TlsAcceptor::from(config),
    }
}

impl Listener for TlsListener {
    fn accept(&self) -> BoxFuture<'_, io::Result<Box<dyn Conn>>> {
        Box::pin(async move {
            let conn = self.inner.accept().await?;
            let (local_addr, remote_addr) = (conn.local_addr(), conn.remote_addr());
            Ok(Box::new(TlsConn {
                state: TlsState::Handshaking(Box::new(self.acceptor.accept(conn))),
                local_addr,
                remote_addr,
            }) as Box<dyn Conn>)
        })
    }

    fn close(&self) -> io::Result<()> {
        self.inner.close()
    }

    fn addr(&self) -> Addr {
        self.inner.addr()
    }
}

/// Go's `net.IPNet`: a network, as `net.ParseCIDR` reads one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IpNet {
    /// The address of the network: nothing set beyond the prefix.
    ip: IpAddr,
    prefix: u32,
}

/// What Go's `net.ParseCIDR` returns.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid CIDR address: {0}")]
pub struct ParseCidrError(pub String);

impl IpNet {
    /// Go's `net.ParseCIDR`, of which only the network is used here.
    pub fn parse_cidr(s: &str) -> Result<IpNet, ParseCidrError> {
        let err = || ParseCidrError(s.to_string());
        let (addr, mask) = s.split_once('/').ok_or_else(err)?;
        let ip: IpAddr = addr.parse().map_err(|_| err())?;
        if mask.is_empty() || mask.len() > 3 || !mask.bytes().all(|b| b.is_ascii_digit()) {
            return Err(err());
        }
        let prefix: u32 = mask.parse().map_err(|_| err())?;
        match ip {
            IpAddr::V4(ip) if prefix <= 32 => Ok(IpNet {
                ip: mask_v4(ip, prefix).into(),
                prefix,
            }),
            IpAddr::V6(ip) if prefix <= 128 => {
                let ip = mask_v6(ip, prefix);
                // A network of IPv4 addresses written as IPv6 addresses is one of IPv4
                // addresses, as `Contains` has it there.
                match ip.to_ipv4_mapped() {
                    Some(ip) if prefix >= 96 => Ok(IpNet {
                        ip: ip.into(),
                        prefix: prefix - 96,
                    }),
                    _ => Ok(IpNet {
                        ip: ip.into(),
                        prefix,
                    }),
                }
            }
            _ => Err(err()),
        }
    }

    /// Go's `Contains`.
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.ip, ip.to_canonical()) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => mask_v4(ip, self.prefix) == net,
            (IpAddr::V6(net), IpAddr::V6(ip)) => mask_v6(ip, self.prefix) == net,
            _ => false,
        }
    }
}

fn mask_v4(ip: Ipv4Addr, prefix: u32) -> Ipv4Addr {
    Ipv4Addr::from(u32::from(ip) & u32::MAX.checked_shl(32 - prefix).unwrap_or(0))
}

fn mask_v6(ip: Ipv6Addr, prefix: u32) -> Ipv6Addr {
    Ipv6Addr::from(u128::from(ip) & u128::MAX.checked_shl(128 - prefix).unwrap_or(0))
}

/// Go's `net.IP.To4() != nil`: an IPv4 address, also one written as an IPv6 address.
pub fn is_ipv4(ip: IpAddr) -> bool {
    matches!(ip.to_canonical(), IpAddr::V4(_))
}
