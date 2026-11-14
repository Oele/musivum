//! The sockets of a node: one UDP socket that all its peer connections share.
//!
//! It stands for what LiveKit sets up in `pkg/rtc/config.go` and pion does underneath: a UDP
//! mux on one port (`rtc.udp_port`), host candidates for the node's address (`node_ip`), no
//! gathering.
//!
//! How a datagram finds its connection (pion's `UDPMux` does the same):
//!
//! - A STUN binding request names its connection by the ICE user name in it, and is given to
//!   that connection wherever it comes from. That is how a client is first heard, and heard
//!   again when its address has changed.
//! - Everything else is given to the connection its sender's address belongs to.
//! - An address belongs to the connection that last sent a STUN message there: an ICE-lite
//!   connection answers a binding request only when it was signed with its password, so an
//!   address is not taken by naming a connection, only by proving to be its other side. A
//!   connection that speaks first (a client's) gets what comes back from where it asks.
//!
//! A node can therefore have many connections that are spoken to, but of connections that
//! speak first only one per remote address: their answers carry no name.
//!
//! One task reads the socket and does nothing but route: each connection has a queue of its
//! own and a task that empties it, so connections do not wait for each other and their work
//! spreads over the runtime's threads. Writing does not go through a task at all: a
//! connection pushes its datagrams into the socket from whatever thread it is called on.
//!
//! A node has a list of listeners with one entry. A second socket (IPv6) is another entry:
//! outgoing datagrams already name the local address and protocol they are for
//! ([`Node::send`]), incoming ones the listener they came in on.
//!
//! # ICE over TCP
//!
//! A node made with [`Node::new_with_tcp`] also listens on a TCP port (LiveKit's
//! `rtc.tcp_port`, pion's TCP mux), and its connections advertise passive TCP host candidates
//! on it. A stream is given to a connection once, by the user name of the binding request
//! that is its first frame, and everything that comes on it after that is the connection's;
//! a stream whose first frame is no such request, or is too long, or comes too late, is
//! closed. What a connection sends over TCP goes to the stream it has with that address. A
//! connection whose settings allow it dials the passive candidates of the other side
//! ([`SettingEngine::active_tcp`](super::SettingEngine::active_tcp)), once for each. A
//! stream is closed when its connection is forgotten, and forgotten when it ends. Framing,
//! limits and buffers are in `tcp.rs`.

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Weak};

use bytes::BytesMut;
use parking_lot::RwLock;
use rtc::shared::{TransportContext, TransportProtocol};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tokio_util::sync::{CancellationToken, DropGuard};

use super::tcp::{self, ACTIVE_TCP_PORT, FrameReader, Writer};
use crate::go::time::Time;
use crate::protocol::logger::{self, Logger};
use crate::{debugw, warnw};

/// The largest datagram that is read whole. WebRTC keeps to the path MTU, which is less.
const MAX_DATAGRAM: usize = 2048;

/// How many datagrams may wait for one connection. One that does not keep up loses what comes
/// after, as with a full socket buffer.
pub(crate) const CONNECTION_QUEUE: usize = 1024;

/// What a node is made from.
#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// The address and port the UDP socket is bound to. Port 0 lets the system choose.
    pub bind_address: SocketAddr,
    /// The addresses given to clients as host candidates, with the socket's port: LiveKit's
    /// `node_ip`, pion's "NAT 1:1" addresses. Empty: the address the socket is bound to.
    pub nat_1to1_ips: Vec<IpAddr>,
    pub logger: Logger,
}

impl Default for NodeConfig {
    /// The loopback interface and a port of the system's choosing, which is what tests want.
    fn default() -> Self {
        NodeConfig {
            bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            nat_1to1_ips: Vec::new(),
            logger: logger::get_logger(),
        }
    }
}

/// A datagram on its way to a connection.
pub(crate) struct Datagram {
    pub(crate) data: BytesMut,
    pub(crate) from: SocketAddr,
    /// The address `rtc` is told it arrived at: the first candidate of the listener.
    pub(crate) local: SocketAddr,
    pub(crate) protocol: TransportProtocol,
    /// When it was read from the socket.
    pub(crate) at: Time,
}

/// Counters of a node, for logs and tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeStats {
    pub datagrams_received: u64,
    /// Datagrams from an address no connection is known for.
    pub datagrams_without_route: u64,
    /// Datagrams dropped because their connection's queue was full.
    pub datagrams_queue_full: u64,
    pub datagrams_sent: u64,
    /// Datagrams the socket did not take (its buffer was full, or an error). Over TCP: a
    /// packet for a stream that is not there or has ended, or that did not fit in its
    /// buffer.
    pub datagrams_not_sent: u64,
    /// TCP streams given to a connection: accepted with the user name of a connection, or
    /// dialled and connected.
    pub tcp_streams_opened: u64,
    /// Of those, the streams that have ended, by either side.
    pub tcp_streams_closed: u64,
    /// Accepted streams that were closed before they were given to a connection.
    pub tcp_streams_refused: u64,
}

struct Listener {
    socket: Arc<UdpSocket>,
    protocol: TransportProtocol,
    local_addr: SocketAddr,
    /// What clients are told: the advertised addresses with the socket's port.
    candidates: Vec<SocketAddr>,
}

/// The TCP listener of a node, as the connections see it.
struct TcpListenerInfo {
    local_addr: SocketAddr,
    /// The passive candidates: the advertised addresses with the listener's port.
    candidates: Vec<SocketAddr>,
}

#[derive(Clone)]
struct Route {
    id: u64,
    tx: mpsc::Sender<Datagram>,
}

struct ConnectionRoutes {
    // The sending end of the connection's queue lives in the routes only: when the
    // connection is forgotten, the queue is closed.
    route: Route,
    ufrags: Vec<String>,
    addrs: Vec<SocketAddr>,
    // The other ends of the connection's TCP streams.
    streams: Vec<SocketAddr>,
    // Whether the connection dials passive candidates.
    active_tcp: bool,
}

/// A TCP stream of a connection, as the routes hold it. Dropping it ends the stream: its
/// writer writes what it has and closes its half, its reader stops.
struct StreamRoute {
    serial: u64,
    writer: Writer,
    _close: DropGuard,
}

#[derive(Default)]
struct Routes {
    by_addr: HashMap<SocketAddr, Route>,
    by_ufrag: HashMap<String, Route>,
    connections: HashMap<u64, ConnectionRoutes>,
    // TCP streams by their connection and the address of their other end.
    streams: HashMap<(u64, SocketAddr), StreamRoute>,
}

struct NodeInner {
    logger: Logger,
    listeners: Vec<Listener>,
    tcp: Option<TcpListenerInfo>,
    routes: RwLock<Routes>,
    next_id: AtomicU64,
    next_stream: AtomicU64,
    // The connections whose task is still running.
    connections: AtomicUsize,
    // Cancelled when the node is closed or dropped: the tasks that read the sockets end, and
    // so do the TCP streams (their tokens are children of this one).
    shutdown: CancellationToken,

    datagrams_received: AtomicU64,
    datagrams_without_route: AtomicU64,
    datagrams_queue_full: AtomicU64,
    datagrams_sent: AtomicU64,
    datagrams_not_sent: AtomicU64,
    tcp_streams_opened: AtomicU64,
    tcp_streams_closed: AtomicU64,
    tcp_streams_refused: AtomicU64,
}

impl Drop for NodeInner {
    fn drop(&mut self) {
        // The tasks that read the sockets hold no reference to this; they are told to end.
        self.shutdown.cancel();
    }
}

/// The sockets of a node. Cheap to clone; the socket is closed when the last clone and the
/// last connection made from it are gone, or by [`Node::close`].
#[derive(Clone)]
pub struct Node {
    inner: Arc<NodeInner>,
}

/// The runtime that reads sockets. `crate::go::time::runtime()` may be one without I/O (the
/// library's own, for callers on plain threads), so reading has a thread of its own.
fn io_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("musivum-net")
            .enable_io()
            .enable_time()
            .build()
            .expect("the network runtime starts")
    });
    &RUNTIME
}

impl Node {
    /// Binds the socket and starts reading it.
    pub fn new(config: NodeConfig) -> io::Result<Node> {
        Node::open(config, None)
    }

    /// Binds the socket and a TCP listener for ICE over TCP on the same address, at
    /// `tcp_port` (LiveKit's `rtc.tcp_port`; 0 lets the system choose), and starts reading
    /// both. The node's connections advertise passive TCP candidates with the listener's port
    /// at the addresses they advertise for UDP, unless their settings leave TCP out
    /// ([`SettingEngine::network_types`](super::SettingEngine::network_types)).
    pub fn new_with_tcp(config: NodeConfig, tcp_port: u16) -> io::Result<Node> {
        Node::open(config, Some(tcp_port))
    }

    fn open(config: NodeConfig, tcp_port: Option<u16>) -> io::Result<Node> {
        let ips = if config.nat_1to1_ips.is_empty() {
            if config.bind_address.ip().is_unspecified() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "a socket bound to all addresses needs an address to advertise",
                ));
            }
            vec![config.bind_address.ip()]
        } else {
            config.nat_1to1_ips.clone()
        };

        let socket = std::net::UdpSocket::bind(config.bind_address)?;
        socket.set_nonblocking(true)?;
        let local_addr = socket.local_addr()?;
        let socket = {
            let _guard = io_runtime().enter();
            Arc::new(UdpSocket::from_std(socket)?)
        };
        let listener = Listener {
            socket: socket.clone(),
            protocol: TransportProtocol::UDP,
            local_addr,
            candidates: ips
                .iter()
                .map(|ip| SocketAddr::new(*ip, local_addr.port()))
                .collect(),
        };
        debugw!(config.logger, "node socket bound", "address" => local_addr, "candidates" => listener.candidates);

        let mut tcp_listener = None;
        let tcp = match tcp_port {
            Some(port) => {
                let std_listener =
                    std::net::TcpListener::bind(SocketAddr::new(config.bind_address.ip(), port))?;
                std_listener.set_nonblocking(true)?;
                let local_addr = std_listener.local_addr()?;
                tcp_listener = {
                    let _guard = io_runtime().enter();
                    Some(TcpListener::from_std(std_listener)?)
                };
                let info = TcpListenerInfo {
                    local_addr,
                    candidates: ips
                        .iter()
                        .map(|ip| SocketAddr::new(*ip, local_addr.port()))
                        .collect(),
                };
                debugw!(config.logger, "node TCP listener bound", "address" => local_addr, "candidates" => info.candidates);
                Some(info)
            }
            None => None,
        };

        let shutdown = CancellationToken::new();
        let inner = Arc::new(NodeInner {
            logger: config.logger,
            listeners: vec![listener],
            tcp,
            routes: RwLock::new(Routes::default()),
            next_id: AtomicU64::new(1),
            next_stream: AtomicU64::new(1),
            connections: AtomicUsize::new(0),
            shutdown: shutdown.clone(),
            datagrams_received: AtomicU64::new(0),
            datagrams_without_route: AtomicU64::new(0),
            datagrams_queue_full: AtomicU64::new(0),
            datagrams_sent: AtomicU64::new(0),
            datagrams_not_sent: AtomicU64::new(0),
            tcp_streams_opened: AtomicU64::new(0),
            tcp_streams_closed: AtomicU64::new(0),
            tcp_streams_refused: AtomicU64::new(0),
        });
        io_runtime().spawn(receive(socket, 0, Arc::downgrade(&inner), shutdown.clone()));
        if let Some(tcp_listener) = tcp_listener {
            io_runtime().spawn(accept(tcp_listener, Arc::downgrade(&inner), shutdown));
        }
        Ok(Node { inner })
    }

    /// The address the UDP socket is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.listeners[0].local_addr
    }

    /// The address the TCP listener is bound to, if the node has one.
    pub fn tcp_local_addr(&self) -> Option<SocketAddr> {
        self.inner.tcp.as_ref().map(|tcp| tcp.local_addr)
    }

    /// The host candidates of this node: what a client is told to send to over UDP.
    pub fn candidates(&self) -> Vec<SocketAddr> {
        self.inner
            .listeners
            .iter()
            .flat_map(|l| l.candidates.iter().copied())
            .collect()
    }

    /// The passive TCP candidates of this node: what a client is told to connect to over
    /// TCP. Empty without a TCP listener.
    pub fn tcp_candidates(&self) -> Vec<SocketAddr> {
        self.inner
            .tcp
            .as_ref()
            .map(|tcp| tcp.candidates.clone())
            .unwrap_or_default()
    }

    /// What a connection that advertises these addresses in the place of the node's own
    /// tells a client to send to: each address with the port of the socket that can be
    /// reached at it. An address of a family the node has no socket for is left out.
    pub(crate) fn candidates_for(&self, ips: &[IpAddr]) -> Vec<SocketAddr> {
        let mut candidates = Vec::new();
        for listener in &self.inner.listeners {
            candidates_at(ips, listener.local_addr, &mut candidates);
        }
        candidates
    }

    /// The same for the TCP listener: the passive candidates of such a connection.
    pub(crate) fn tcp_candidates_for(&self, ips: &[IpAddr]) -> Vec<SocketAddr> {
        let mut candidates = Vec::new();
        if let Some(tcp) = &self.inner.tcp {
            candidates_at(ips, tcp.local_addr, &mut candidates);
        }
        candidates
    }

    /// How many connections of this node are alive: made and not yet closed and wound up.
    pub fn connection_count(&self) -> usize {
        self.inner.connections.load(Ordering::SeqCst)
    }

    pub fn stats(&self) -> NodeStats {
        let i = &self.inner;
        NodeStats {
            datagrams_received: i.datagrams_received.load(Ordering::Relaxed),
            datagrams_without_route: i.datagrams_without_route.load(Ordering::Relaxed),
            datagrams_queue_full: i.datagrams_queue_full.load(Ordering::Relaxed),
            datagrams_sent: i.datagrams_sent.load(Ordering::Relaxed),
            datagrams_not_sent: i.datagrams_not_sent.load(Ordering::Relaxed),
            tcp_streams_opened: i.tcp_streams_opened.load(Ordering::Relaxed),
            tcp_streams_closed: i.tcp_streams_closed.load(Ordering::Relaxed),
            tcp_streams_refused: i.tcp_streams_refused.load(Ordering::Relaxed),
        }
    }

    /// Stops reading the socket and the TCP listener, closes the TCP streams and lets go of
    /// the connections: each of them that is still open finds its queue closed and closes
    /// itself.
    pub fn close(&self) {
        self.inner.shutdown.cancel();
        let mut routes = self.inner.routes.write();
        routes.by_addr.clear();
        routes.by_ufrag.clear();
        routes.connections.clear();
        let streams = std::mem::take(&mut routes.streams);
        drop(routes);
        drop(streams);
    }

    /// Makes room for a connection: its number, and the queue its datagrams are put in.
    /// `active_tcp`: the connection dials the passive TCP candidates it sends to.
    pub(crate) fn register(
        &self,
        active_tcp: bool,
    ) -> (u64, mpsc::Receiver<Datagram>, ConnectionGuard) {
        let id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel(CONNECTION_QUEUE);
        self.inner.routes.write().connections.insert(
            id,
            ConnectionRoutes {
                route: Route { id, tx },
                ufrags: Vec::new(),
                addrs: Vec::new(),
                streams: Vec::new(),
                active_tcp,
            },
        );
        self.inner.connections.fetch_add(1, Ordering::SeqCst);
        let guard = ConnectionGuard {
            node: self.clone(),
            id,
        };
        (id, rx, guard)
    }

    /// Datagrams with this ICE user name are for this connection from now on.
    pub(crate) fn add_ufrag(&self, id: u64, ufrag: &str) {
        let mut routes = self.inner.routes.write();
        let Some(connection) = routes.connections.get_mut(&id) else {
            return;
        };
        if connection.ufrags.iter().any(|u| u == ufrag) {
            return;
        }
        connection.ufrags.push(ufrag.to_string());
        let route = connection.route.clone();
        routes.by_ufrag.insert(ufrag.to_string(), route);
    }

    /// A connection sends a STUN message to this address: what comes from there is for it
    /// from now on.
    fn learn_addr(&self, id: u64, addr: SocketAddr) {
        // Nearly always it is known already.
        if self
            .inner
            .routes
            .read()
            .by_addr
            .get(&addr)
            .is_some_and(|r| r.id == id)
        {
            return;
        }
        let mut routes = self.inner.routes.write();
        let Some(connection) = routes.connections.get_mut(&id) else {
            return;
        };
        if !connection.addrs.contains(&addr) {
            connection.addrs.push(addr);
        }
        let route = connection.route.clone();
        let previous = routes.by_addr.insert(addr, route);
        drop(routes);
        debugw!(
            self.inner.logger,
            "connection is at a new address",
            "address" => addr,
            "connection" => id,
            "previousConnection" => previous.map(|r| r.id)
        );
    }

    /// Forgets a connection. Its queue is closed by this, and its TCP streams.
    pub(crate) fn unregister(&self, id: u64) {
        let mut routes = self.inner.routes.write();
        let Some(connection) = routes.connections.remove(&id) else {
            return;
        };
        for ufrag in connection.ufrags {
            if routes.by_ufrag.get(&ufrag).is_some_and(|r| r.id == id) {
                routes.by_ufrag.remove(&ufrag);
            }
        }
        for addr in connection.addrs {
            if routes.by_addr.get(&addr).is_some_and(|r| r.id == id) {
                routes.by_addr.remove(&addr);
            }
        }
        let streams: Vec<StreamRoute> = connection
            .streams
            .iter()
            .filter_map(|peer| routes.streams.remove(&(id, *peer)))
            .collect();
        drop(routes);
        // Ending a stream takes no lock of the node, but it is let go of outside anyway.
        drop(streams);
    }

    /// Pushes a datagram of a connection into the socket it is for, now. One the socket does
    /// not take is lost, as UDP allows.
    pub(crate) fn send(&self, id: u64, transport: &TransportContext, data: &[u8]) {
        if transport.transport_protocol == TransportProtocol::TCP {
            self.send_tcp(id, transport, data);
            return;
        }
        // STUN, as against DTLS and RTP (RFC 7983): the first octet is below 4.
        if data.first().is_some_and(|first| *first < 4) {
            self.learn_addr(id, transport.peer_addr);
        }
        let listener = self.inner.listener_for(transport);
        match listener.socket.try_send_to(data, transport.peer_addr) {
            Ok(_) => {
                self.inner.datagrams_sent.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.inner
                    .datagrams_not_sent
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Puts a packet of a connection into the buffer of its TCP stream to that address. When
    /// there is none and the packet is from the connection's active candidate (a check on a
    /// pair with a passive candidate of the other side), the connection dials that address
    /// first, if it may; the packet waits in the stream's buffer meanwhile.
    fn send_tcp(&self, id: u64, transport: &TransportContext, data: &[u8]) {
        let inner = &self.inner;
        let not_sent = || {
            inner.datagrams_not_sent.fetch_add(1, Ordering::Relaxed);
        };
        let Some(frame) = tcp::frame(data) else {
            return not_sent();
        };
        let peer = transport.peer_addr;
        let written = |stream: &StreamRoute, frame| {
            if stream.writer.write(frame) {
                inner.datagrams_sent.fetch_add(1, Ordering::Relaxed);
            } else {
                not_sent();
            }
        };
        {
            let routes = inner.routes.read();
            if let Some(stream) = routes.streams.get(&(id, peer)) {
                return written(stream, frame);
            }
            let may_dial = transport.local_addr.port() == ACTIVE_TCP_PORT
                && routes.connections.get(&id).is_some_and(|c| c.active_tcp);
            if !may_dial {
                return not_sent();
            }
        }

        let mut routes = inner.routes.write();
        // Another thread of the connection may have dialled in between.
        if let Some(stream) = routes.streams.get(&(id, peer)) {
            return written(stream, frame);
        }
        let Some(connection) = routes.connections.get_mut(&id) else {
            return not_sent();
        };
        connection.streams.push(peer);
        let to_connection = connection.route.tx.downgrade();
        let (writer, queue) = Writer::new();
        let closed = inner.shutdown.child_token();
        let stream = StreamRoute {
            serial: inner.next_stream.fetch_add(1, Ordering::Relaxed),
            writer,
            _close: closed.clone().drop_guard(),
        };
        written(&stream, frame);
        routes.streams.insert((id, peer), stream);
        drop(routes);
        debugw!(inner.logger, "dialling a passive TCP candidate", "connection" => id, "address" => peer);
        let end = StreamEnd {
            node: Arc::downgrade(inner),
            to_connection,
            connection: id,
            serial: None,
            peer,
            local: transport.local_addr,
        };
        io_runtime().spawn(dial(end, queue, closed));
    }
}

/// Where the packets of a TCP stream go, and what is forgotten when it ends.
struct StreamEnd {
    node: Weak<NodeInner>,
    to_connection: mpsc::WeakSender<Datagram>,
    connection: u64,
    // The stream's number for a stream that was accepted, which is forgotten when it ends.
    // A dialled one stays in the routes, ended, until its connection is forgotten: it is
    // dialled once, as pion's agent dials a passive candidate once.
    serial: Option<u64>,
    peer: SocketAddr,
    // The address `rtc` is told the packets arrived at.
    local: SocketAddr,
}

impl StreamEnd {
    /// Gives a packet to the connection, waiting while its queue is full (as pion's TCP mux
    /// waits: TCP slows the other side down). False when the connection is gone.
    async fn deliver(&self, data: BytesMut, closed: &CancellationToken) -> bool {
        let Some(node) = self.node.upgrade() else {
            return false;
        };
        node.datagrams_received.fetch_add(1, Ordering::Relaxed);
        drop(node);
        let Some(tx) = self.to_connection.upgrade() else {
            return false;
        };
        let datagram = Datagram {
            data,
            from: self.peer,
            local: self.local,
            protocol: TransportProtocol::TCP,
            at: Time::now(),
        };
        tokio::select! {
            _ = closed.cancelled() => false,
            res = tx.send(datagram) => res.is_ok(),
        }
    }

    /// Reads the stream until it ends, or until its route is dropped or the node closed.
    async fn read(
        &self,
        stream: OwnedReadHalf,
        mut reader: FrameReader,
        closed: CancellationToken,
    ) {
        loop {
            let res = tokio::select! {
                _ = closed.cancelled() => break,
                res = reader.next(&stream, tcp::FRAME_LIMIT) => res,
            };
            match res {
                Ok(Some(data)) => {
                    if !data.is_empty() && !self.deliver(data, &closed).await {
                        break;
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    if let Some(node) = self.node.upgrade() {
                        debugw!(node.logger, "TCP stream failed", "error" => err.to_string(), "connection" => self.connection, "address" => self.peer);
                    }
                    break;
                }
            }
        }
        // The writer of a dialled stream, which stays in the routes, stops too.
        closed.cancel();
        self.ended();
    }

    /// Counts the stream as ended, and forgets an accepted one: dropping its route lets its
    /// writer finish.
    fn ended(&self) {
        let Some(node) = self.node.upgrade() else {
            return;
        };
        node.tcp_streams_closed.fetch_add(1, Ordering::Relaxed);
        if let Some(serial) = self.serial {
            let mut routes = node.routes.write();
            let key = (self.connection, self.peer);
            if routes.streams.get(&key).is_some_and(|s| s.serial == serial) {
                let stream = routes.streams.remove(&key);
                if let Some(connection) = routes.connections.get_mut(&self.connection) {
                    connection.streams.retain(|p| *p != self.peer);
                }
                drop(routes);
                drop(stream);
            }
        }
    }
}

/// Dials a passive candidate for a connection, then writes and reads the stream.
async fn dial(end: StreamEnd, queue: tcp::WriterQueue, closed: CancellationToken) {
    let res = tokio::select! {
        _ = closed.cancelled() => return,
        res = tokio::time::timeout(tcp::DIAL_TIMEOUT, TcpStream::connect(end.peer)) => res,
    };
    let Some(node) = end.node.upgrade() else {
        return;
    };
    let stream = match res {
        Ok(Ok(stream)) => stream,
        // The queue is dropped: what is written to the stream from now on is lost.
        Ok(Err(err)) => {
            debugw!(node.logger, "could not dial a passive TCP candidate", "error" => err.to_string(), "address" => end.peer);
            return;
        }
        Err(_) => {
            debugw!(node.logger, "could not dial a passive TCP candidate in time", "address" => end.peer);
            return;
        }
    };
    node.tcp_streams_opened.fetch_add(1, Ordering::Relaxed);
    drop(node);
    let _ = stream.set_nodelay(true);
    let (read, write) = stream.into_split();
    tokio::spawn(tcp::write_loop(write, queue, closed.clone()));
    end.read(read, FrameReader::default(), closed).await;
}

/// Accepts TCP streams until the node is closed or dropped.
async fn accept(listener: TcpListener, node: Weak<NodeInner>, shutdown: CancellationToken) {
    loop {
        let res = tokio::select! {
            _ = shutdown.cancelled() => return,
            res = listener.accept() => res,
        };
        match res {
            Ok((stream, peer)) => {
                tokio::spawn(serve(stream, peer, node.clone(), shutdown.child_token()));
            }
            Err(err) => {
                // Out of descriptors, mostly: said, and tried again a little later.
                let Some(node) = node.upgrade() else { return };
                warnw!(node.logger, "could not accept a TCP stream", err);
                drop(node);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

/// Gives an accepted stream to the connection its first binding request names, then writes
/// and reads it. pion's `TCPMuxDefault.handleConn`.
async fn serve(
    stream: TcpStream,
    peer: SocketAddr,
    node: Weak<NodeInner>,
    closed: CancellationToken,
) {
    let _ = stream.set_nodelay(true);
    let local_addr = stream.local_addr().ok();
    let (read, write) = stream.into_split();
    let mut reader = FrameReader::default();
    let first = tokio::select! {
        _ = closed.cancelled() => return,
        res = tokio::time::timeout(
            tcp::FIRST_STUN_BIND_TIMEOUT,
            reader.next(&read, tcp::FIRST_FRAME_LIMIT),
        ) => res,
    };
    let Some(inner) = node.upgrade() else {
        return;
    };
    let refuse = |reason: &str| {
        inner.tcp_streams_refused.fetch_add(1, Ordering::Relaxed);
        debugw!(inner.logger, "TCP stream refused", "reason" => reason, "address" => peer, "local" => local_addr);
        // Dropping both halves closes the stream.
    };
    let first = match first {
        Ok(Ok(Some(first))) => first,
        Ok(Ok(None)) => return refuse("closed before its first binding request"),
        Ok(Err(err)) => return refuse(&err.to_string()),
        Err(_) => return refuse("no binding request in time"),
    };
    let Some(ufrag) = stun_binding_request_username(&first) else {
        return refuse("the first packet is not a binding request with a user name");
    };

    let (id, to_connection, serial, queue) = match inner.adopt(ufrag, peer, &closed) {
        Ok(adopted) => adopted,
        Err(reason) => return refuse(reason),
    };
    inner.tcp_streams_opened.fetch_add(1, Ordering::Relaxed);
    let local = inner
        .tcp
        .as_ref()
        .map(|tcp| tcp.candidates[0])
        .unwrap_or(peer);
    debugw!(inner.logger, "TCP stream accepted", "connection" => id, "address" => peer);
    drop(inner);

    tokio::spawn(tcp::write_loop(write, queue, closed.clone()));
    let end = StreamEnd {
        node,
        to_connection,
        connection: id,
        serial: Some(serial),
        peer,
        local,
    };
    if end.deliver(first, &closed).await {
        end.read(read, reader, closed).await;
    } else {
        end.ended();
    }
}

/// Counts a connection as alive for as long as its task holds this.
pub(crate) struct ConnectionGuard {
    node: Node,
    id: u64,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        // However the task ends (closed, or dropped with its runtime), nothing is routed to
        // it any longer.
        self.node.unregister(self.id);
        self.node.inner.connections.fetch_sub(1, Ordering::SeqCst);
    }
}

impl NodeInner {
    fn listener_for(&self, transport: &TransportContext) -> &Listener {
        let local = transport.local_addr;
        self.listeners
            .iter()
            .find(|l| l.protocol == transport.transport_protocol && l.candidates.contains(&local))
            // A connection with addresses of its own: they have the port of their socket.
            .or_else(|| {
                self.listeners.iter().find(|l| {
                    l.protocol == transport.transport_protocol
                        && l.local_addr.port() == local.port()
                        && l.local_addr.is_ipv4() == local.is_ipv4()
                })
            })
            .unwrap_or(&self.listeners[0])
    }

    /// Gives a datagram to the connection it is for.
    fn route(&self, datagram: Datagram) {
        self.datagrams_received.fetch_add(1, Ordering::Relaxed);
        let routes = self.routes.read();
        // A binding request names its connection. Whether the name is rightly used the
        // connection finds out (the request is signed); the address becomes the connection's
        // when it answers.
        let route = stun_binding_request_username(&datagram.data)
            .and_then(|ufrag| routes.by_ufrag.get(ufrag))
            .or_else(|| routes.by_addr.get(&datagram.from));
        match route {
            Some(route) => self.deliver(route, datagram),
            None => {
                self.datagrams_without_route.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn deliver(&self, route: &Route, datagram: Datagram) {
        if route.tx.try_send(datagram).is_err() {
            self.datagrams_queue_full.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Gives an accepted TCP stream to the connection with this ICE user name: the
    /// connection's number and queue, the stream's number, and the buffer its writer
    /// empties. Or why not.
    #[allow(clippy::type_complexity)]
    fn adopt(
        &self,
        ufrag: &str,
        peer: SocketAddr,
        closed: &CancellationToken,
    ) -> Result<(u64, mpsc::WeakSender<Datagram>, u64, tcp::WriterQueue), &'static str> {
        let mut routes = self.routes.write();
        let id = routes
            .by_ufrag
            .get(ufrag)
            .map(|r| r.id)
            .ok_or("no connection has that user name")?;
        if routes.streams.contains_key(&(id, peer)) {
            return Err("the connection has a stream from that address");
        }
        let connection = routes
            .connections
            .get_mut(&id)
            .ok_or("the connection is gone")?;
        connection.streams.push(peer);
        let to_connection = connection.route.tx.downgrade();
        let serial = self.next_stream.fetch_add(1, Ordering::Relaxed);
        let (writer, queue) = Writer::new();
        routes.streams.insert(
            (id, peer),
            StreamRoute {
                serial,
                writer,
                _close: closed.clone().drop_guard(),
            },
        );
        Ok((id, to_connection, serial, queue))
    }
}

/// Reads one socket until the node is closed or dropped.
async fn receive(
    socket: Arc<UdpSocket>,
    listener: usize,
    node: Weak<NodeInner>,
    shutdown: CancellationToken,
) {
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        let res = tokio::select! {
            _ = shutdown.cancelled() => return,
            res = socket.recv_from(&mut buf) => res,
        };
        let Some(node) = node.upgrade() else { return };
        match res {
            Ok((n, from)) => {
                let l = &node.listeners[listener];
                node.route(Datagram {
                    data: BytesMut::from(&buf[..n]),
                    from,
                    local: l.candidates[0],
                    protocol: l.protocol,
                    at: Time::now(),
                });
            }
            Err(err) => {
                // On Linux a UDP socket reports here that something it sent was refused
                // (ICMP); reading goes on. Anything else is said once in a while.
                if err.kind() != io::ErrorKind::ConnectionRefused
                    && err.kind() != io::ErrorKind::ConnectionReset
                {
                    warnw!(node.logger, "could not read the node's socket", err);
                    drop(node);
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }
}

/// Each address with the port of a socket bound at `local`, if it is of the socket's family,
/// added to `candidates` unless it is there.
fn candidates_at(ips: &[IpAddr], local: SocketAddr, candidates: &mut Vec<SocketAddr>) {
    for ip in ips {
        let candidate = SocketAddr::new(ip.to_canonical(), local.port());
        if candidate.is_ipv4() == local.is_ipv4() && !candidates.contains(&candidate) {
            candidates.push(candidate);
        }
    }
}

/// The local part of the USERNAME of a STUN binding request ("ours:theirs"), if this is one.
fn stun_binding_request_username(data: &[u8]) -> Option<&str> {
    // Binding request: type 0x0001; then the length, the magic cookie and the transaction.
    if data.len() < 20 || data[0] != 0x00 || data[1] != 0x01 {
        return None;
    }
    if data[4..8] != [0x21, 0x12, 0xA4, 0x42] {
        return None;
    }
    let len = u16::from_be_bytes([data[2], data[3]]) as usize;
    let mut rest = data.get(20..20 + len)?;
    while rest.len() >= 4 {
        let typ = u16::from_be_bytes([rest[0], rest[1]]);
        let alen = u16::from_be_bytes([rest[2], rest[3]]) as usize;
        let value = rest.get(4..4 + alen)?;
        if typ == 0x0006 {
            let username = std::str::from_utf8(value).ok()?;
            return username.split(':').next();
        }
        let padded = (alen + 3) & !3;
        rest = rest.get(4 + padded..)?;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_user_name_of_a_binding_request_is_found() {
        // type, length 16, cookie, transaction id, then PRIORITY (8 bytes) and USERNAME
        // "abcd:wxyz" (9 bytes, padded to 12).
        let mut msg = vec![0x00, 0x01, 0x00, 0x18, 0x21, 0x12, 0xA4, 0x42];
        msg.extend_from_slice(&[7u8; 12]);
        msg.extend_from_slice(&[0x00, 0x24, 0x00, 0x04, 1, 2, 3, 4]);
        msg.extend_from_slice(&[0x00, 0x06, 0x00, 0x09]);
        msg.extend_from_slice(b"abcd:wxyz\0\0\0");
        assert_eq!(stun_binding_request_username(&msg), Some("abcd"));

        // A binding response is not a request, and media is not STUN.
        let mut response = msg.clone();
        response[0] = 0x01;
        response[1] = 0x01;
        assert_eq!(stun_binding_request_username(&response), None);
        assert_eq!(stun_binding_request_username(&[0x80; 40]), None);
        // Cut short: nothing is read beyond what is there.
        assert_eq!(stun_binding_request_username(&msg[..30]), None);
    }

    #[test]
    fn a_socket_on_all_addresses_needs_an_address_to_advertise() {
        let err = Node::new(NodeConfig {
            bind_address: "0.0.0.0:0".parse().unwrap(),
            ..Default::default()
        })
        .err()
        .expect("no address to tell clients");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let node = Node::new(NodeConfig {
            bind_address: "0.0.0.0:0".parse().unwrap(),
            nat_1to1_ips: vec!["127.0.0.1".parse().unwrap()],
            ..Default::default()
        })
        .unwrap();
        let port = node.local_addr().port();
        assert_ne!(port, 0);
        assert_eq!(
            node.candidates(),
            vec![SocketAddr::new("127.0.0.1".parse().unwrap(), port)]
        );
        node.close();
    }

    /// A binding request with this user name for the connection.
    fn binding_request(ufrag: &str) -> Vec<u8> {
        let username = format!("{ufrag}:them");
        let padded = (username.len() + 3) & !3;
        let mut msg = vec![0x00, 0x01, 0x00, (4 + padded) as u8, 0x21, 0x12, 0xA4, 0x42];
        msg.extend_from_slice(&[7u8; 12]);
        msg.extend_from_slice(&[0x00, 0x06, 0x00, username.len() as u8]);
        msg.extend_from_slice(username.as_bytes());
        msg.resize(20 + 4 + padded, 0);
        msg
    }

    async fn next(queue: &mut mpsc::Receiver<Datagram>) -> Option<Vec<u8>> {
        tokio::time::timeout(std::time::Duration::from_millis(300), queue.recv())
            .await
            .ok()
            .flatten()
            .map(|datagram| datagram.data.to_vec())
    }

    /// Waits until the node has counted this many datagrams that were for nobody.
    async fn without_route(node: &Node, count: u64) {
        crate::testutils::eventually_async(
            || node.stats().datagrams_without_route == count,
            std::time::Duration::from_secs(5),
            std::time::Duration::from_millis(5),
        )
        .await;
    }

    /// The rules by which a datagram finds its connection: a binding request by the name in
    /// it, everything else by the address, and an address goes to the connection that
    /// answered from it, not to one that was merely named.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_datagram_finds_its_connection() {
        let node = Node::new(NodeConfig::default()).unwrap();
        let (first, mut first_queue, first_guard) = node.register(false);
        let (second, mut second_queue, second_guard) = node.register(false);
        node.add_ufrag(first, "one1");
        node.add_ufrag(second, "two2");
        assert_eq!(node.connection_count(), 2);

        let client = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let to_client = TransportContext {
            local_addr: node.candidates()[0],
            peer_addr: client.local_addr().unwrap(),
            transport_protocol: TransportProtocol::UDP,
            ecn: None,
        };
        let media = [0x80u8; 20];
        let response = [0x01u8, 0x01, 0, 0];

        // Nobody is known at that address yet.
        client.send_to(&media, node.local_addr()).unwrap();
        assert_eq!(next(&mut first_queue).await, None);
        without_route(&node, 1).await;

        // A binding request is for the connection it names; being named is not yet being at
        // that address.
        client
            .send_to(&binding_request("one1"), node.local_addr())
            .unwrap();
        assert_eq!(next(&mut first_queue).await, Some(binding_request("one1")));
        client.send_to(&media, node.local_addr()).unwrap();
        assert_eq!(next(&mut first_queue).await, None);
        without_route(&node, 2).await;

        // The connection answers: from now on the address is its own. Media it sends there
        // changes nothing.
        node.send(first, &to_client, &response);
        client.send_to(&media, node.local_addr()).unwrap();
        assert_eq!(next(&mut first_queue).await, Some(media.to_vec()));

        // Somebody names the other connection from this address (it takes no more than
        // knowing a user name). The request goes where it says, the address stays.
        client
            .send_to(&binding_request("two2"), node.local_addr())
            .unwrap();
        assert_eq!(next(&mut second_queue).await, Some(binding_request("two2")));
        node.send(second, &to_client, &media);
        client.send_to(&media, node.local_addr()).unwrap();
        assert_eq!(next(&mut first_queue).await, Some(media.to_vec()));
        assert_eq!(next(&mut second_queue).await, None);

        // Only when the other connection answers there is the address its own.
        node.send(second, &to_client, &response);
        client.send_to(&media, node.local_addr()).unwrap();
        assert_eq!(next(&mut second_queue).await, Some(media.to_vec()));
        assert_eq!(next(&mut first_queue).await, None);

        // What the node sent arrived, and a connection that is gone gets nothing.
        let mut buf = [0u8; 64];
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        assert_eq!(client.recv_from(&mut buf).unwrap().0, response.len());
        assert_eq!(node.stats().datagrams_sent, 3);
        drop(second_guard);
        assert_eq!(node.connection_count(), 1);
        client.send_to(&media, node.local_addr()).unwrap();
        assert_eq!(next(&mut second_queue).await, None);
        without_route(&node, 3).await;
        drop(first_guard);
        assert_eq!(node.connection_count(), 0);
        node.close();
    }
}
