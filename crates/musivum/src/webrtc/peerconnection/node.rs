//! The sockets of a node, which all its peer connections share.
//!
//! It stands for what LiveKit sets up in `pkg/rtc/config.go` and pion does underneath: a UDP
//! mux (`rtc.udp_port`), host candidates for the node's addresses (`node_ip`), no gathering.
//!
//! # Sockets
//!
//! A node listens at each of its addresses ([`NodeConfig::addresses`]), IPv4 and IPv6 alike,
//! with a socket of its own for each address, as LiveKit's UDP mux does
//! (`CreateUDPMuxesFromPorts` binds every local address it keeps): what a connection sends
//! then leaves from the address the other side sent to. One socket on all addresses would
//! answer from whichever address the system picks for the way back, and on a host with
//! several addresses that is not always the one the client knows; a client takes no answer
//! from an address it did not send to (RFC 8445, 7.2.5.2.1), and neither does a node, which
//! routes by address.
//!
//! At each address there is a socket for each port of [`NodeConfig::udp_ports`]; with more
//! than one port, every new connection is given one of them at each address, in turn
//! (LiveKit's `udp_port` as a range, `NewMultiPortsUDPMux`), so that the connections spread
//! over the sockets and the tasks that read them. The sockets on the standalone ports
//! ([`NodeConfig::standalone_udp_ports`], LiveKit's `use_stun_port_as_ice`) are every
//! connection's.
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
//! A task for each socket reads it and does nothing but route: each connection has a queue
//! of its own and a task that empties it, so connections do not wait for each other and
//! their work spreads over the runtime's threads. Writing does not go through a task at all:
//! a connection pushes its datagrams into the socket of the candidate they are sent from,
//! from whatever thread it is called on. The routing is the node's, over all its sockets:
//! a binding request finds its connection whichever socket it came in on, and the connection
//! tells `rtc` it arrived at its candidate on that socket.
//!
//! # ICE over TCP
//!
//! A node made with [`Node::new_with_tcp`] also listens on a TCP port (LiveKit's
//! `rtc.tcp_port`, pion's TCP mux), at each of its addresses, and its connections advertise
//! passive TCP host candidates on it. A stream is given to a connection once, by the user name of the binding request
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
    /// The addresses the node listens at, each with what clients are told for it. Each has
    /// sockets of its own: one for each port of `udp_ports` and of `standalone_udp_ports`,
    /// and a TCP listener on a node made with [`Node::new_with_tcp`].
    pub addresses: Vec<NodeAddress>,
    /// The ports of the UDP sockets at each address.
    pub udp_ports: UdpPorts,
    /// Ports on which there is a UDP socket at each address besides, whose candidates every
    /// connection has: LiveKit's `use_stun_port_as_ice` (3478). 0 lets the system choose.
    pub standalone_udp_ports: Vec<u16>,
    pub logger: Logger,
}

impl Default for NodeConfig {
    /// The IPv4 loopback address and a port of the system's choosing, which is what tests
    /// want.
    fn default() -> Self {
        NodeConfig {
            addresses: vec![NodeAddress::new(IpAddr::V4(Ipv4Addr::LOCALHOST))],
            udp_ports: UdpPorts::default(),
            standalone_udp_ports: Vec::new(),
            logger: logger::get_logger(),
        }
    }
}

/// An address a node listens at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeAddress {
    /// The address the sockets are bound to. The unspecified address of a family binds them
    /// to every address of that family, and an answer then leaves from the address the
    /// system chooses; such an address needs addresses to advertise.
    pub local: IpAddr,
    /// The addresses given to clients for it as host candidates, each with the port of each
    /// socket: LiveKit's `node_ip`, pion's "NAT 1:1" addresses (the rewrite rules for host
    /// candidates). Empty: `local` itself.
    pub advertised: Vec<IpAddr>,
}

impl NodeAddress {
    /// An address that is advertised as it is.
    pub fn new(local: IpAddr) -> NodeAddress {
        NodeAddress {
            local,
            advertised: Vec::new(),
        }
    }
}

/// The ports of a node's UDP sockets at each of its addresses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UdpPorts {
    /// A socket on each of these ports; 0 lets the system choose one. A port that cannot be
    /// bound is an error. With more than one, each connection is given one of the sockets at
    /// each address, in turn: LiveKit's `udp_port` as a range, of which it binds as many
    /// ports as the machine has processors.
    Ports(Vec<u16>),
    /// Sockets on the first `count` ports from `start` to `end` (both included) that can be
    /// bound, each connection given one of them at each address, in turn: LiveKit's
    /// `port_range_start` and `port_range_end`, where pion opens a port of the range for
    /// every connection. A port that cannot be bound is passed over, as pion passes over
    /// the ports of the range that are taken; none at all is an error.
    Range { start: u16, end: u16, count: usize },
}

impl Default for UdpPorts {
    /// One socket, on a port of the system's choosing.
    fn default() -> Self {
        UdpPorts::Ports(vec![0])
    }
}

/// A host candidate of a connection: an address it is advertised at, and the socket (UDP)
/// or listener (TCP) behind it, by its number on the node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct HostCandidate {
    pub(crate) addr: SocketAddr,
    pub(crate) listener: usize,
}

/// A datagram on its way to a connection.
pub(crate) struct Datagram {
    pub(crate) data: BytesMut,
    pub(crate) from: SocketAddr,
    /// The address `rtc` is told it arrived at when the connection has no candidate at its
    /// listener: the first candidate of the listener, or for a stream the connection
    /// dialled, its active candidate.
    pub(crate) local: SocketAddr,
    /// The UDP socket or TCP listener it came in at, by its number; none for a stream the
    /// connection dialled.
    pub(crate) listener: Option<usize>,
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

/// A UDP socket of the node.
struct Listener {
    // The same socket for writing, as the system's: tokio's `try_send_to` does not try
    // before the runtime has seen the socket writable, which a socket that was just bound
    // has not always been, and a datagram would be lost that the system takes. `None` once
    // the node is closed, so that the port is let go of then.
    sender: RwLock<Option<std::net::UdpSocket>>,
    local_addr: SocketAddr,
    /// What clients are told: the advertised addresses with the socket's port.
    candidates: Vec<SocketAddr>,
}

/// A TCP listener of the node, as the connections see it.
struct TcpListenerInfo {
    local_addr: SocketAddr,
    /// The passive candidates: the advertised addresses with the listener's port.
    candidates: Vec<SocketAddr>,
}

/// The sockets of one address on the ports of `udp_ports`, by their numbers, and which of
/// them the next connection is given.
struct PortGroup {
    listeners: Vec<usize>,
    next: AtomicUsize,
}

/// Whether a connection can be reached at `ip` through a socket bound at `local` that is
/// advertised as `candidates`: it is one of those, or the bound address, or of the family of
/// a socket bound to all addresses of a family.
fn reachable_at(ip: IpAddr, local: SocketAddr, candidates: &[SocketAddr]) -> bool {
    candidates.iter().any(|c| c.ip() == ip)
        || local.ip() == ip
        || (local.ip().is_unspecified() && local.is_ipv4() == ip.is_ipv4())
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
    // The UDP sockets, in the order of the addresses: at each, the ports of `udp_ports`,
    // then the standalone ports.
    listeners: Vec<Listener>,
    // For each address, its sockets on the ports of `udp_ports`.
    groups: Vec<PortGroup>,
    // The sockets on the standalone ports, which every connection has.
    standalone: Vec<usize>,
    // The TCP listeners, one for each address; none without a TCP port.
    tcp: Vec<TcpListenerInfo>,
    routes: RwLock<Routes>,
    next_id: AtomicU64,
    next_stream: AtomicU64,
    // The connections whose task is still running.
    connections: AtomicUsize,
    // Cancelled when the node is closed or dropped: the tasks that read the sockets end, and
    // so do the TCP streams (their tokens are children of this one).
    shutdown: CancellationToken,
    // Cancelled when the last of the tasks that read the sockets and accept TCP streams has
    // ended, and with it let go of its socket or listener (`SocketTasks`).
    sockets_closed: CancellationToken,

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

// Held by every task that reads a socket or accepts TCP streams: when the last of them has
// ended, the node's sockets and listeners are closed.
struct SocketTasks(CancellationToken);

impl Drop for SocketTasks {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// The sockets of a node. Cheap to clone; the socket is closed when the last clone and the
/// last connection made from it are gone, or by [`Node::close`].
#[derive(Clone)]
pub struct Node {
    inner: Arc<NodeInner>,
}

/// The runtime that reads sockets. `crate::go::time::runtime()` may be one without I/O (the
/// library's own, for callers on plain threads), so reading has threads of its own: as many
/// as the machine has processors, so that the sockets of a node with several ports are read
/// at the same time.
fn io_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(std::thread::available_parallelism().map_or(1, |n| n.get()))
            .thread_name("musivum-net")
            .enable_io()
            .enable_time()
            .build()
            .expect("the network runtime starts")
    });
    &RUNTIME
}

impl Node {
    /// Binds the sockets and starts reading them.
    pub fn new(config: NodeConfig) -> io::Result<Node> {
        Node::open(config, None)
    }

    /// Binds the sockets and a TCP listener for ICE over TCP at each address, on `tcp_port`
    /// (LiveKit's `rtc.tcp_port`; 0 lets the system choose, for each address its own), and
    /// starts reading them. The node's connections advertise passive TCP candidates with a
    /// listener's port at the addresses they advertise for it, unless their settings leave
    /// TCP out ([`SettingEngine::network_types`](super::SettingEngine::network_types)).
    pub fn new_with_tcp(config: NodeConfig, tcp_port: u16) -> io::Result<Node> {
        Node::open(config, Some(tcp_port))
    }

    fn open(config: NodeConfig, tcp_port: Option<u16>) -> io::Result<Node> {
        if config.addresses.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a node needs an address to listen at",
            ));
        }
        let mut listeners = Vec::new();
        let mut sockets = Vec::new();
        let mut groups = Vec::new();
        let mut standalone = Vec::new();
        let mut tcp = Vec::new();
        let mut tcp_listeners = Vec::new();
        let mut bind = |local: SocketAddr, ips: &[IpAddr]| -> io::Result<usize> {
            let (socket, listener) = bind_udp(local, ips)?;
            debugw!(config.logger, "node socket bound", "address" => listener.local_addr, "candidates" => listener.candidates);
            sockets.push(socket);
            listeners.push(listener);
            Ok(listeners.len() - 1)
        };
        for address in &config.addresses {
            let ips = if address.advertised.is_empty() {
                if address.local.is_unspecified() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "a socket bound to all addresses needs an address to advertise",
                    ));
                }
                vec![address.local]
            } else {
                address.advertised.clone()
            };

            let mut group = Vec::new();
            match &config.udp_ports {
                UdpPorts::Ports(ports) => {
                    for port in ports {
                        group.push(bind(SocketAddr::new(address.local, *port), &ips)?);
                    }
                }
                UdpPorts::Range { start, end, count } => {
                    let mut last_err = None;
                    for port in *start..=*end {
                        if group.len() >= (*count).max(1) {
                            break;
                        }
                        match bind(SocketAddr::new(address.local, port), &ips) {
                            Ok(listener) => group.push(listener),
                            Err(err) => last_err = Some(err),
                        }
                    }
                    if group.is_empty() {
                        return Err(last_err.unwrap_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidInput, "the port range is empty")
                        }));
                    }
                }
            }
            groups.push(PortGroup {
                listeners: group,
                next: AtomicUsize::new(0),
            });
            for port in &config.standalone_udp_ports {
                standalone.push(bind(SocketAddr::new(address.local, *port), &ips)?);
            }

            if let Some(port) = tcp_port {
                let std_listener =
                    std::net::TcpListener::bind(SocketAddr::new(address.local, port))?;
                std_listener.set_nonblocking(true)?;
                let local_addr = std_listener.local_addr()?;
                let listener = {
                    let _guard = io_runtime().enter();
                    TcpListener::from_std(std_listener)?
                };
                let info = TcpListenerInfo {
                    local_addr,
                    candidates: ips
                        .iter()
                        .map(|ip| SocketAddr::new(*ip, local_addr.port()))
                        .collect(),
                };
                debugw!(config.logger, "node TCP listener bound", "address" => local_addr, "candidates" => info.candidates);
                tcp_listeners.push(listener);
                tcp.push(info);
            }
        }
        if listeners.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a node needs a UDP port",
            ));
        }

        let shutdown = CancellationToken::new();
        let sockets_closed = CancellationToken::new();
        let socket_tasks = Arc::new(SocketTasks(sockets_closed.clone()));
        let inner = Arc::new(NodeInner {
            logger: config.logger,
            listeners,
            groups,
            standalone,
            tcp,
            routes: RwLock::new(Routes::default()),
            next_id: AtomicU64::new(1),
            next_stream: AtomicU64::new(1),
            connections: AtomicUsize::new(0),
            shutdown: shutdown.clone(),
            sockets_closed,
            datagrams_received: AtomicU64::new(0),
            datagrams_without_route: AtomicU64::new(0),
            datagrams_queue_full: AtomicU64::new(0),
            datagrams_sent: AtomicU64::new(0),
            datagrams_not_sent: AtomicU64::new(0),
            tcp_streams_opened: AtomicU64::new(0),
            tcp_streams_closed: AtomicU64::new(0),
            tcp_streams_refused: AtomicU64::new(0),
        });
        for (i, socket) in sockets.into_iter().enumerate() {
            let tasks = socket_tasks.clone();
            let receiving = receive(socket, i, Arc::downgrade(&inner), shutdown.clone());
            io_runtime().spawn(async move {
                receiving.await;
                drop(tasks);
            });
        }
        for (i, listener) in tcp_listeners.into_iter().enumerate() {
            let tasks = socket_tasks.clone();
            let accepting = accept(listener, i, Arc::downgrade(&inner), shutdown.clone());
            io_runtime().spawn(async move {
                accepting.await;
                drop(tasks);
            });
        }
        Ok(Node { inner })
    }

    /// The address the first UDP socket is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.listeners[0].local_addr
    }

    /// The addresses all UDP sockets are bound to, in the order of the node's addresses: at
    /// each, the ports of `udp_ports`, then the standalone ports.
    pub fn local_addrs(&self) -> Vec<SocketAddr> {
        self.inner.listeners.iter().map(|l| l.local_addr).collect()
    }

    /// The address the first TCP listener is bound to, if the node has one.
    pub fn tcp_local_addr(&self) -> Option<SocketAddr> {
        self.inner.tcp.first().map(|tcp| tcp.local_addr)
    }

    /// The addresses the TCP listeners are bound to, one for each address of the node; none
    /// without a TCP port.
    pub fn tcp_local_addrs(&self) -> Vec<SocketAddr> {
        self.inner.tcp.iter().map(|tcp| tcp.local_addr).collect()
    }

    /// The host candidates of all sockets of this node: what clients are told to send to over
    /// UDP. A connection has some of them ([`UdpPorts`]).
    pub fn candidates(&self) -> Vec<SocketAddr> {
        let mut candidates = Vec::new();
        for c in self.inner.listeners.iter().flat_map(|l| &l.candidates) {
            if !candidates.contains(c) {
                candidates.push(*c);
            }
        }
        candidates
    }

    /// The passive TCP candidates of this node: what a client is told to connect to over
    /// TCP. Empty without a TCP listener.
    pub fn tcp_candidates(&self) -> Vec<SocketAddr> {
        let mut candidates = Vec::new();
        for c in self.inner.tcp.iter().flat_map(|l| &l.candidates) {
            if !candidates.contains(c) {
                candidates.push(*c);
            }
        }
        candidates
    }

    /// The UDP candidates of a new connection: at each address one of its sockets on the
    /// ports of `udp_ports`, in turn, and the sockets on the standalone ports, each with the
    /// addresses it is advertised at. With `ips`, the connection advertises those in the
    /// place of the node's own: each at every socket of the connection that can be reached
    /// at it (one it is advertised at, or the one bound to it); an address no socket can be
    /// reached at is left out. A candidate that is there already is not added again, as
    /// pion's agent leaves out a candidate it has.
    pub(crate) fn connection_candidates(&self, ips: Option<&[IpAddr]>) -> Vec<HostCandidate> {
        let inner = &self.inner;
        let mut chosen = Vec::new();
        for group in inner.groups.iter().filter(|g| !g.listeners.is_empty()) {
            let i = group.next.fetch_add(1, Ordering::Relaxed) % group.listeners.len();
            chosen.push(group.listeners[i]);
        }
        chosen.extend(&inner.standalone);
        let mut candidates = Vec::new();
        for i in chosen {
            let listener = &inner.listeners[i];
            candidates_at(
                ips,
                i,
                listener.local_addr,
                &listener.candidates,
                &mut candidates,
            );
        }
        candidates
    }

    /// The passive TCP candidates of a new connection: those of every TCP listener, with
    /// `ips` as for UDP.
    pub(crate) fn connection_tcp_candidates(&self, ips: Option<&[IpAddr]>) -> Vec<HostCandidate> {
        let mut candidates = Vec::new();
        for (i, listener) in self.inner.tcp.iter().enumerate() {
            candidates_at(
                ips,
                i,
                listener.local_addr,
                &listener.candidates,
                &mut candidates,
            );
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
    /// itself. The sockets and listeners are closed as soon as the tasks that read them have
    /// ended ([`Node::closed`]), whoever still holds the node; nothing is sent after.
    pub fn close(&self) {
        self.inner.shutdown.cancel();
        for listener in &self.inner.listeners {
            listener.sender.write().take();
        }
        let mut routes = self.inner.routes.write();
        routes.by_addr.clear();
        routes.by_ufrag.clear();
        routes.connections.clear();
        let streams = std::mem::take(&mut routes.streams);
        drop(routes);
        drop(streams);
    }

    /// Waits until the node's sockets and TCP listeners are closed, after [`Node::close`]:
    /// their ports can then be bound again.
    pub async fn closed(&self) {
        self.inner.sockets_closed.cancelled().await;
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

    /// Pushes a datagram of a connection into the socket it is for, now: over UDP the socket
    /// `listener` (the one behind the connection's candidate it is sent from), or when that
    /// is not known, the one whose candidates have the local address. One the socket does not
    /// take is lost, as UDP allows.
    pub(crate) fn send(
        &self,
        id: u64,
        listener: Option<usize>,
        transport: &TransportContext,
        data: &[u8],
    ) {
        if transport.transport_protocol == TransportProtocol::TCP {
            self.send_tcp(id, transport, data);
            return;
        }
        // STUN, as against DTLS and RTP (RFC 7983): the first octet is below 4.
        if data.first().is_some_and(|first| *first < 4) {
            self.learn_addr(id, transport.peer_addr);
        }
        let listener = match listener.and_then(|l| self.inner.listeners.get(l)) {
            Some(listener) => listener,
            None => self.inner.listener_for(transport),
        };
        let sent = match &*listener.sender.read() {
            Some(sender) => sender.send_to(data, transport.peer_addr),
            None => Err(io::ErrorKind::NotConnected.into()),
        };
        match sent {
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
            listener: None,
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
    // The address `rtc` is told the packets arrived at when the connection has no candidate
    // at the listener: the listener's first, or the active candidate of a dialled stream.
    local: SocketAddr,
    // The listener that accepted the stream; none for a dialled one.
    listener: Option<usize>,
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
            listener: self.listener,
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

/// Accepts TCP streams at the listener with this number until the node is closed or
/// dropped.
async fn accept(
    listener: TcpListener,
    index: usize,
    node: Weak<NodeInner>,
    shutdown: CancellationToken,
) {
    loop {
        let res = tokio::select! {
            _ = shutdown.cancelled() => return,
            res = listener.accept() => res,
        };
        match res {
            Ok((stream, peer)) => {
                tokio::spawn(serve(
                    stream,
                    peer,
                    index,
                    node.clone(),
                    shutdown.child_token(),
                ));
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
    listener: usize,
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
    let local = inner.tcp[listener].candidates[0];
    debugw!(inner.logger, "TCP stream accepted", "connection" => id, "address" => peer, "local" => local_addr);
    drop(inner);

    tokio::spawn(tcp::write_loop(write, queue, closed.clone()));
    let end = StreamEnd {
        node,
        to_connection,
        connection: id,
        serial: Some(serial),
        peer,
        local,
        listener: Some(listener),
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
    /// The UDP socket a datagram from this local address leaves from, when the connection
    /// did not say: the first whose candidates have it, or that is bound to it with its port.
    fn listener_for(&self, transport: &TransportContext) -> &Listener {
        let local = transport.local_addr;
        self.listeners
            .iter()
            .find(|l| l.candidates.contains(&local))
            .or_else(|| {
                self.listeners.iter().find(|l| {
                    l.local_addr.port() == local.port()
                        && reachable_at(local.ip(), l.local_addr, &l.candidates)
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
    socket: UdpSocket,
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
                    listener: Some(listener),
                    protocol: TransportProtocol::UDP,
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

/// Binds a UDP socket for the node at `local`, advertised at `ips`.
fn bind_udp(local: SocketAddr, ips: &[IpAddr]) -> io::Result<(UdpSocket, Listener)> {
    let socket = std::net::UdpSocket::bind(local)?;
    socket.set_nonblocking(true)?;
    let local_addr = socket.local_addr()?;
    let sender = socket.try_clone()?;
    let socket = {
        let _guard = io_runtime().enter();
        UdpSocket::from_std(socket)?
    };
    let listener = Listener {
        sender: RwLock::new(Some(sender)),
        local_addr,
        candidates: ips
            .iter()
            .map(|ip| SocketAddr::new(*ip, local_addr.port()))
            .collect(),
    };
    Ok((socket, listener))
}

/// Adds to `candidates` those of a connection at the socket or listener `listener`, bound at
/// `local` and advertised as `advertised`: the advertised ones, or with `ips` each of those
/// the socket can be reached at, with its port. One that is there is not added again.
fn candidates_at(
    ips: Option<&[IpAddr]>,
    listener: usize,
    local: SocketAddr,
    advertised: &[SocketAddr],
    candidates: &mut Vec<HostCandidate>,
) {
    let mut add = |addr: SocketAddr| {
        if !candidates.iter().any(|c| c.addr == addr) {
            candidates.push(HostCandidate { addr, listener });
        }
    };
    match ips {
        None => advertised.iter().copied().for_each(&mut add),
        Some(ips) => {
            for ip in ips.iter().map(IpAddr::to_canonical) {
                if reachable_at(ip, local, advertised) {
                    add(SocketAddr::new(ip, local.port()));
                }
            }
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
            addresses: vec![NodeAddress::new("0.0.0.0".parse().unwrap())],
            ..Default::default()
        })
        .err()
        .expect("no address to tell clients");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let node = Node::new(NodeConfig {
            addresses: vec![NodeAddress {
                local: "0.0.0.0".parse().unwrap(),
                advertised: vec!["127.0.0.1".parse().unwrap()],
            }],
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
        node.send(first, None, &to_client, &response);
        client.send_to(&media, node.local_addr()).unwrap();
        assert_eq!(next(&mut first_queue).await, Some(media.to_vec()));

        // Somebody names the other connection from this address (it takes no more than
        // knowing a user name). The request goes where it says, the address stays.
        client
            .send_to(&binding_request("two2"), node.local_addr())
            .unwrap();
        assert_eq!(next(&mut second_queue).await, Some(binding_request("two2")));
        node.send(second, None, &to_client, &media);
        client.send_to(&media, node.local_addr()).unwrap();
        assert_eq!(next(&mut first_queue).await, Some(media.to_vec()));
        assert_eq!(next(&mut second_queue).await, None);

        // Only when the other connection answers there is the address its own.
        node.send(second, None, &to_client, &response);
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
