//! A TURN client for the tests of the server: what of pion/turn's client (`client.go`,
//! `internal/client`) the tests use, written for them over a UDP socket or a stream (TCP,
//! TLS). It sends one request at a time and waits for its answer (no retransmissions: the
//! tests run on the loopback interface); it authenticates as pion's client does (a request
//! without credentials, then again with the nonce and realm of the 401), and makes a
//! permission before it relays to a peer, with Send indications (pion's client binds a channel
//! as well, which the tests that need one do themselves).

use std::collections::{HashSet, VecDeque};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};

use super::generate_auth_key;
use super::internal::proto::{
    self, ChannelData, Data, PROTO_UDP, PeerAddress, RelayedAddress, RequestedTransport,
    consume_single_turn_frame,
};
use super::internal::server::crypto;
use crate::webrtc::stun::message::{
    BINDING_REQUEST, CLASS_ERROR_RESPONSE, CLASS_INDICATION, CLASS_REQUEST, Getter,
    METHOD_ALLOCATE, METHOD_CREATE_PERMISSION, METHOD_DATA, METHOD_SEND, Message, MessageType,
    Method, Setter, TransactionId, is_stun_message,
};
use crate::webrtc::stun::xoraddr::XorMappedAddress;
use rtc::stun::attributes::{ATTR_NONCE, ATTR_REALM, ATTR_USERNAME};
use rtc::stun::error_code::ErrorCodeAttribute;
use rtc::stun::fingerprint::FINGERPRINT;
use rtc::stun::integrity::MessageIntegrity;
use rtc::stun::textattrs::TextAttribute;

pub(crate) trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}

enum Transport {
    Udp(tokio::net::UdpSocket),
    Stream {
        r: ReadHalf<Box<dyn Io>>,
        w: WriteHalf<Box<dyn Io>>,
        buf: Vec<u8>,
    },
}

/// What came from the server that was not the answer to a request.
#[derive(Debug)]
#[allow(dead_code)] // what a test may look at
pub(crate) enum Incoming {
    Data(SocketAddr, Vec<u8>),
    ChannelData(u16, Vec<u8>),
    Stun(Message),
}

pub(crate) struct TestClient {
    transport: Transport,
    pub local_addr: SocketAddr,
    pub server_addr: SocketAddr,
    username: String,
    password: String,
    realm: String,
    nonce: String,
    pending: VecDeque<Incoming>,
    permissions: HashSet<IpAddr>,
}

pub(crate) const TIMEOUT: Duration = Duration::from_secs(5);

/// A message's ERROR-CODE, if it has one.
pub(crate) fn error_code_of(m: &Message) -> Option<u16> {
    let mut code = ErrorCodeAttribute::default();
    code.get_from(m).ok().map(|_| code.code.0)
}

impl TestClient {
    /// A client on a new UDP socket of the loopback interface.
    pub(crate) async fn udp(server_addr: SocketAddr, username: &str, password: &str) -> TestClient {
        let bind: SocketAddr = if server_addr.is_ipv4() {
            "127.0.0.1:0".parse().unwrap()
        } else {
            "[::1]:0".parse().unwrap()
        };
        let socket = tokio::net::UdpSocket::bind(bind).await.unwrap();
        let local_addr = socket.local_addr().unwrap();
        TestClient::new(
            Transport::Udp(socket),
            local_addr,
            server_addr,
            username,
            password,
        )
    }

    /// A client on a stream (TCP, TLS) to the server.
    pub(crate) fn stream(
        stream: Box<dyn Io>,
        local_addr: SocketAddr,
        server_addr: SocketAddr,
        username: &str,
        password: &str,
    ) -> TestClient {
        let (r, w) = tokio::io::split(stream);
        TestClient::new(
            Transport::Stream {
                r,
                w,
                buf: Vec::new(),
            },
            local_addr,
            server_addr,
            username,
            password,
        )
    }

    fn new(
        transport: Transport,
        local_addr: SocketAddr,
        server_addr: SocketAddr,
        username: &str,
        password: &str,
    ) -> TestClient {
        TestClient {
            transport,
            local_addr,
            server_addr,
            username: username.to_string(),
            password: password.to_string(),
            realm: String::new(),
            nonce: String::new(),
            pending: VecDeque::new(),
            permissions: HashSet::new(),
        }
    }

    pub(crate) async fn send(&mut self, b: &[u8]) -> io::Result<()> {
        match &mut self.transport {
            Transport::Udp(socket) => socket.send_to(b, self.server_addr).await.map(|_| ()),
            Transport::Stream { w, .. } => w.write_all(b).await,
        }
    }

    async fn recv_raw(&mut self, timeout: Duration) -> io::Result<Vec<u8>> {
        let read = async {
            match &mut self.transport {
                Transport::Udp(socket) => {
                    let mut b = vec![0u8; 2048];
                    let (n, _) = socket.recv_from(&mut b).await?;
                    b.truncate(n);
                    Ok(b)
                }
                Transport::Stream { r, buf, .. } => loop {
                    if let Ok(n) = consume_single_turn_frame(buf) {
                        return Ok(buf.drain(..n).collect());
                    }
                    let mut b = vec![0u8; 2048];
                    let n = r.read(&mut b).await?;
                    if n == 0 {
                        return Err(io::ErrorKind::UnexpectedEof.into());
                    }
                    buf.extend_from_slice(&b[..n]);
                },
            }
        };
        tokio::time::timeout(timeout, read)
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
    }

    fn parse(b: Vec<u8>) -> Incoming {
        if is_stun_message(&b) {
            let mut m = Message::new();
            m.raw = b;
            if m.decode().is_ok() {
                if m.typ == MessageType::new(METHOD_DATA, CLASS_INDICATION) {
                    let (mut peer, mut data) = (PeerAddress::default(), Data::default());
                    if peer.get_from(&m).is_ok() && data.get_from(&m).is_ok() {
                        return Incoming::Data(SocketAddr::new(peer.ip, peer.port), data.0);
                    }
                }
                return Incoming::Stun(m);
            }
            return Incoming::Stun(Message::new());
        }
        let mut c = ChannelData {
            raw: b,
            ..Default::default()
        };
        let _ = c.decode();
        Incoming::ChannelData(c.number.0, c.data)
    }

    /// What came from the server, in order: a Data indication, ChannelData or a STUN message.
    pub(crate) async fn recv(&mut self, timeout: Duration) -> Result<Incoming, String> {
        if let Some(i) = self.pending.pop_front() {
            return Ok(i);
        }
        let b = self.recv_raw(timeout).await.map_err(|e| e.to_string())?;
        Ok(TestClient::parse(b))
    }

    /// Sends a message made of `setters` and waits for the answer to its transaction.
    pub(crate) async fn request(&mut self, setters: &[&dyn Setter]) -> Result<Message, String> {
        let msg = proto::build(setters).map_err(|e| e.to_string())?;
        self.send(&msg.raw).await.map_err(|e| e.to_string())?;
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            let b = self.recv_raw(left).await.map_err(|e| e.to_string())?;
            match TestClient::parse(b) {
                Incoming::Stun(m) if m.transaction_id == msg.transaction_id => return Ok(m),
                other => self.pending.push_back(other),
            }
        }
    }

    /// A request of `method` with `attrs`, authenticated as pion's client does.
    pub(crate) async fn authenticated(
        &mut self,
        method: Method,
        attrs: &[&dyn Setter],
    ) -> Result<Message, String> {
        for _ in 0..3 {
            let tid = TransactionId::new();
            let typ = MessageType::new(method, CLASS_REQUEST);
            let username = TextAttribute::new(ATTR_USERNAME, self.username.clone());
            let realm = TextAttribute::new(ATTR_REALM, self.realm.clone());
            let nonce = TextAttribute::new(ATTR_NONCE, self.nonce.clone());
            let integrity = MessageIntegrity::new_raw_integrity_with_provider(
                generate_auth_key(&self.username, &self.realm, &self.password),
                crypto(),
            );
            let mut setters: Vec<&dyn Setter> = vec![&tid, &typ];
            setters.extend_from_slice(attrs);
            if !self.nonce.is_empty() {
                setters.extend_from_slice(&[&username, &realm, &nonce, &integrity]);
            }
            setters.push(&FINGERPRINT);
            let resp = self.request(&setters).await?;
            if matches!(error_code_of(&resp), Some(401) | Some(438))
                && let Ok(n) = TextAttribute::get_from_as(&resp, ATTR_NONCE)
            {
                self.nonce = n.text;
                if let Ok(r) = TextAttribute::get_from_as(&resp, ATTR_REALM) {
                    self.realm = r.text;
                }
                continue;
            }
            return Ok(resp);
        }
        Err("authentication did not succeed".to_string())
    }

    /// pion's `SendBindingRequest`: the address the server sees.
    pub(crate) async fn send_binding_request(&mut self) -> Result<SocketAddr, String> {
        let tid = TransactionId::new();
        let resp = self.request(&[&tid, &BINDING_REQUEST]).await?;
        let mut a = XorMappedAddress::default();
        a.get_from(&resp).map_err(|e| e.to_string())?;
        Ok(SocketAddr::new(a.ip, a.port))
    }

    /// An Allocate request with these attributes besides the credentials: the answer.
    pub(crate) async fn allocate_with(&mut self, attrs: &[&dyn Setter]) -> Result<Message, String> {
        self.authenticated(METHOD_ALLOCATE, attrs).await
    }

    /// pion's `Allocate`: the relayed address of a UDP relay.
    pub(crate) async fn allocate(&mut self) -> Result<SocketAddr, String> {
        let resp = self
            .allocate_with(&[&RequestedTransport {
                protocol: PROTO_UDP,
            }])
            .await?;
        if resp.typ.class == CLASS_ERROR_RESPONSE {
            return Err(format!("{} (error {:?})", resp.typ, error_code_of(&resp)));
        }
        let mut relayed = RelayedAddress::default();
        relayed.get_from(&resp).map_err(|e| e.to_string())?;
        Ok(SocketAddr::new(relayed.ip, relayed.port))
    }

    /// pion's `CreatePermission(addrs...)`: one request for all of them.
    pub(crate) async fn create_permission(&mut self, peers: &[SocketAddr]) -> Result<(), String> {
        let addrs: Vec<PeerAddress> = peers
            .iter()
            .map(|p| PeerAddress {
                ip: p.ip(),
                port: p.port(),
            })
            .collect();
        let setters: Vec<&dyn Setter> = addrs.iter().map(|a| a as &dyn Setter).collect();
        let resp = self
            .authenticated(METHOD_CREATE_PERMISSION, &setters)
            .await?;
        if resp.typ.class == CLASS_ERROR_RESPONSE {
            return Err(format!("{} (error {:?})", resp.typ, error_code_of(&resp)));
        }
        for p in peers {
            self.permissions.insert(p.ip());
        }
        Ok(())
    }

    pub(crate) async fn send_indication(
        &mut self,
        data: &[u8],
        peer: SocketAddr,
    ) -> Result<(), String> {
        let msg = proto::build(&[
            &TransactionId::new(),
            &MessageType::new(METHOD_SEND, CLASS_INDICATION),
            &Data(data.to_vec()),
            &PeerAddress {
                ip: peer.ip(),
                port: peer.port(),
            },
            &FINGERPRINT,
        ])
        .map_err(|e| e.to_string())?;
        self.send(&msg.raw).await.map_err(|e| e.to_string())
    }

    /// pion's relay connection's `WriteTo`: a permission first, then a Send indication.
    pub(crate) async fn write_to(&mut self, data: &[u8], peer: SocketAddr) -> Result<(), String> {
        if !self.permissions.contains(&peer.ip()) {
            self.create_permission(&[peer]).await?;
        }
        self.send_indication(data, peer).await
    }

    /// pion's relay connection's `ReadFrom`: the next data relayed from a peer.
    pub(crate) async fn read_from(
        &mut self,
        timeout: Duration,
    ) -> Result<(Vec<u8>, SocketAddr), String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if let Incoming::Data(from, data) = self.recv(left).await? {
                return Ok((data, from));
            }
        }
    }
}
