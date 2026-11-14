// Ported from LiveKit, pkg/service/server.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, on tokio and axum, for one node.
// - The services are not injected one by one (`NewLivekitServer` takes eighteen). What the
//   routes share is `ServerState`; the routes are made by `new_mux` (the original's `mux`,
//   built inside `NewLivekitServer`), where the room API and the signalling endpoint will be
//   added, and handed to `LivekitServer::new`. `initialize_server` stands in for wire's
//   `InitializeServer` (wire_gen.go) for what is ported: the bus of the process, the signal
//   and room manager clients, the router of one node (with the defaults of the node's
//   statistics, as `musivum::config` does not read `node_stats`), the key provider
//   (`createKeyProvider` of wire.go) and the WebRTC configuration and sockets.
// - Not here, with what they belong to: the Twirp servers of egress, ingress, SIP and agent
//   dispatch, WHIP, the agent endpoints and their tunnels (`drainAgentTunnels`), the IO info
//   service, WebTransport, TURN, the signal server between nodes, the listeners for
//   Prometheus and for debugging, and the debug handlers of development mode
//   (`debugGoroutines`, which is Go's runtime, and `debugInfo`, which reads the room
//   manager). Of `NewHTTPHandler`'s middleware, negroni's recovery and the CORS handler of
//   rs/cors are not here (a handler that panics ends its connection; a browser on another
//   origin cannot read the answers); the request body limit, the API key authentication and
//   the path normalisation are.
// - What waits on the room manager is not here yet: the wait for participants in `Stop`,
//   `roomManager.Stop()` at the end of `Start`, `backgroundWorker` (which closes idle rooms)
//   and `RoomManager()`. A comment stands at each place.
// - `Start` and `Stop` are async. The done channel is made by `new` (a cancelled token cannot
//   be made anew, and the server is started once). `httpServer.Shutdown` with its timeout of
//   five seconds is the graceful shutdown of axum's server with that timeout: no new
//   connections, the requests in progress are answered; a WebSocket, once upgraded, is not
//   waited for, as Go's `Shutdown` does not wait for a hijacked connection. A server bound to
//   no address in particular listens on `[::]` (with IPv4 on the same socket, as Linux gives
//   it), or on `0.0.0.0` where there is no IPv6, where Go listens on both. `local_addrs` is
//   new: the addresses listened on, for a port of 0. The node of the WebRTC connections is
//   closed at the end of `Start` (new: upstream leaves its sockets to the end of the
//   process).
// - The health check gives the time of the node's last update in UTC; the original gives it
//   in the server's time zone.
// - Go's `ServeMux` redirects a path with `..` or a doubled slash inside it to its clean
//   form; axum's router does not, and such a path is not found.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::middleware::{Next, from_fn};
use axum::response::{IntoResponse, Response};
use musivum::config::Config;
use musivum::errorw;
use musivum::go::atomic;
use musivum::go::time::{self, Duration, Time};
use musivum::mediatransportutil::rtcconfig;
use musivum::protocol::auth::{FileBasedKeyProvider, KeyProvider};
use musivum::protocol::logger;
use musivum::routing::{self, LocalNode, MessageBus, Router};
use musivum::rtc;
use musivum::service::{ApiKeyAuthMiddleware, RemoteAddr};
use musivum::webrtc::peerconnection::Node;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::utils::{RequestBodyLimiter, with_path_normalization};

// the time `httpServer.Shutdown` is given
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
// how old the node's statistics may be for the node to be ready
const HEALTH_CHECK_MAX_AGE: Duration = Duration::from_secs(4);

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("already running")]
    AlreadyRunning,
    #[error("one of key-file or keys must be provided in order to support a secure installation")]
    KeysNotProvided,
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Routing(#[from] routing::Error),
    #[error(transparent)]
    RtcConfig(#[from] rtcconfig::Error),
}

/// What the routes of the node share. The signalling endpoint and the room API take what
/// they need from here (the room manager, when it is ported, joins it).
#[derive(Clone)]
pub struct ServerState {
    pub config: Arc<Config>,
    pub current_node: Arc<dyn LocalNode>,
    /// The bus of the process, which the signal server and the room manager are registered on
    /// (psrpc's local bus upstream).
    pub message_bus: Arc<MessageBus>,
    pub router: Arc<dyn Router>,
    pub key_provider: Arc<dyn KeyProvider>,
    /// `rtc.NewWebRTCConfig(conf)`: what every WebRTC connection of the node is made with.
    pub webrtc_config: Arc<rtc::WebRtcConfig>,
    /// The sockets of the node's WebRTC connections, made once from `webrtc_config`.
    pub rtc_node: Node,
}

/// wire's `InitializeServer`, for what is ported: the router of one node and what it is made
/// with, the key provider, the WebRTC configuration and the node's sockets, the routes. It
/// blocks (the WebRTC configuration may ask STUN servers for the node's address): call it
/// before the runtime or from `spawn_blocking`.
pub fn initialize_server(
    conf: Config,
    current_node: Arc<dyn LocalNode>,
) -> Result<Arc<LivekitServer>, Error> {
    let node_id = current_node.node_id();
    // getMessageBus without Redis
    let message_bus = routing::new_local_message_bus();
    let signal_client = routing::new_signal_client(node_id, message_bus.clone())?;
    let room_manager_client =
        routing::new_room_manager_client(message_bus.clone(), conf.room.clone())?;
    let router = routing::create_router(
        current_node.clone(),
        signal_client,
        room_manager_client,
        routing::default_node_stats_config(),
    );
    let key_provider = create_key_provider(&conf)?;
    let webrtc_config = rtc::WebRtcConfig::new(&conf)?;
    let rtc_node = webrtc_config.web_rtc_config.new_node()?;
    let state = ServerState {
        config: Arc::new(conf),
        current_node,
        message_bus,
        router,
        key_provider,
        webrtc_config: Arc::new(webrtc_config),
        rtc_node,
    };
    let mux = new_mux(state.clone());
    LivekitServer::new(state, mux)
}

// createKeyProvider of wire.go. The key file has been read into `conf.keys` by
// `Config::validate_keys` when the server started, with the same check of its permissions;
// the original reads it a second time here.
fn create_key_provider(conf: &Config) -> Result<Arc<dyn KeyProvider>, Error> {
    if conf.keys.is_empty() {
        return Err(Error::KeysNotProvided);
    }

    Ok(Arc::new(FileBasedKeyProvider::new_from_map(
        conf.keys.clone(),
    )))
}

/// The routes of the node: the original's `mux`. Everything that is not routed to anything
/// else reaches `default_handler`, which answers the health check on `/`.
pub fn new_mux(state: ServerState) -> axum::Router {
    // In development mode the original adds pprof, /debug/goroutine and /debug/rooms: not
    // here (see the header).
    axum::Router::new()
        // HERE GO THE ROOM API AND THE SIGNALLING ENDPOINT, as routes of this router:
        // - the room API, `xtwirp.RegisterServer(mux, roomServer)`: roomservice.go over Twirp,
        //   under `/twirp/livekit.RoomService/`;
        // - the signalling endpoint, `rtcService.SetupRoutes(mux)`: rtcservice.go, at `/rtc`,
        //   `/rtc/v1` and their `/validate`. Its WebSocket is `super::AxumSignalWebSocket`,
        //   made after `super::set_read_limit(ws, conf.limit.signal_message_size_limit)`.
        // Egress, ingress, SIP, agent dispatch, WHIP and `/agent` are left out.
        .fallback(default_handler)
        .with_state(state)
}

pub struct LivekitServer {
    config: Arc<Config>,
    state: ServerState,
    http_server: axum::Router,
    router: Arc<dyn Router>,
    current_node: Arc<dyn LocalNode>,
    rtc_node: Node,
    running: atomic::Bool,
    done_chan: CancellationToken,
    closed_chan: CancellationToken,
    local_addrs: Mutex<Vec<SocketAddr>>,
}

impl LivekitServer {
    /// `NewLivekitServer`, with the routes made (`new_mux`).
    pub fn new(state: ServerState, mux: axum::Router) -> Result<Arc<LivekitServer>, Error> {
        let conf = state.config.clone();
        let http_server = new_http_handler(&conf, Some(state.key_provider.clone()), mux);

        // prometheus_port, prometheus, debug_handler and webtransport are taken by the
        // configuration and not read (see the header).

        state.router.remove_dead_nodes()?;

        Ok(Arc::new(LivekitServer {
            config: conf,
            current_node: state.current_node.clone(),
            rtc_node: state.rtc_node.clone(),
            router: state.router.clone(),
            state,
            http_server,
            running: atomic::Bool::new(false),
            done_chan: CancellationToken::new(),
            closed_chan: CancellationToken::new(),
            local_addrs: Mutex::new(Vec::new()),
        }))
    }

    pub fn node(&self) -> musivum::protocol::livekit::Node {
        self.current_node.clone_node()
    }

    pub fn http_port(&self) -> i64 {
        self.config.port as i64
    }

    pub fn is_running(&self) -> bool {
        self.running.load()
    }

    /// What the routes share.
    pub fn state(&self) -> &ServerState {
        &self.state
    }

    /// The addresses the HTTP server listens on, once it does.
    pub fn local_addrs(&self) -> Vec<SocketAddr> {
        self.local_addrs.lock().clone()
    }

    /// Serves until `stop` is called. Needs a tokio runtime with I/O and time.
    pub async fn start(self: &Arc<Self>) -> Result<(), Error> {
        if self.running.load() {
            return Err(Error::AlreadyRunning);
        }

        self.router.register_node()?;
        let _unregister = UnregisterNode(self.router.as_ref());

        self.router.start()?;

        // the IO info service: not here

        let mut addresses = self.config.bind_addresses.clone();
        if addresses.is_empty() {
            addresses = vec![String::new()];
        }

        // ensure we could listen
        let mut listeners = Vec::new();
        for addr in &addresses {
            let ln = listen(addr, self.config.port).await?;
            self.local_addrs.lock().push(ln.local_addr()?);
            listeners.push(ln);

            // the listeners for Prometheus and for debugging: not here
        }

        // WebTransport: not here

        {
            let rtc = &self.config.rtc.rtc_config;
            let node_id = self.current_node.node_id();
            let node_ip = self.current_node.node_ip();
            let mut values: Vec<(&str, &dyn std::fmt::Debug)> = vec![
                ("portHttp", &self.config.port),
                ("nodeID", &node_id),
                ("nodeIP", &node_ip),
                ("version", &VERSION),
            ];
            if !self.config.bind_addresses.is_empty() {
                values.push(("bindAddresses", &self.config.bind_addresses));
            }
            if rtc.tcp_port != 0 {
                values.push(("rtc.portTCP", &rtc.tcp_port));
            }
            let ice_port_range = [rtc.ice_port_range_start, rtc.ice_port_range_end];
            if !rtc.force_tcp && rtc.udp_port.valid() {
                values.push(("rtc.portUDP", &rtc.udp_port));
            } else {
                values.push(("rtc.portICERange", &ice_port_range));
            }
            if !self.config.region.is_empty() {
                values.push(("region", &self.config.region));
            }
            logger::get_logger().infow("starting LiveKit server", &values);
        }

        // the signal server between nodes: not here

        let shutdown = CancellationToken::new();
        let mut http_group = tokio::task::JoinSet::new();
        for ln in listeners {
            let service = self
                .http_server
                .clone()
                .into_make_service_with_connect_info::<SocketAddr>();
            let shutdown = shutdown.clone();
            http_group.spawn(async move {
                axum::serve(ln, service)
                    .with_graceful_shutdown(shutdown.cancelled_owned())
                    .await
            });
        }
        let http_closed = CancellationToken::new();
        {
            let s = self.clone();
            let http_closed = http_closed.clone();
            tokio::spawn(async move {
                let mut first_err = None;
                while let Some(res) = http_group.join_next().await {
                    let res = res.unwrap_or_else(|err| Err(io::Error::other(err)));
                    if let Err(err) = res
                        && first_err.is_none()
                    {
                        first_err = Some(err);
                    }
                }
                http_closed.cancel();
                if let Some(err) = first_err {
                    errorw!(logger::get_logger(), "could not start server", err);
                    s.stop(true).await;
                }
            });
        }

        // backgroundWorker, which closes idle rooms every second: with the room manager

        // give time for Serve goroutine to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        self.running.store(true);

        self.done_chan.cancelled().await;

        // the agents' tunnels: not here

        // wait for shutdown
        shutdown.cancel();
        let _ = tokio::time::timeout(SHUTDOWN_TIMEOUT.to_std(), http_closed.cancelled()).await;

        // the debug server, WebTransport and TURN: not here

        // roomManager.Stop(), signalServer.Stop() and ioService.Stop(): with the room manager
        // and the signalling endpoint
        self.rtc_node.close();

        self.closed_chan.cancel();
        Ok(())
    }

    pub async fn stop(&self, force: bool) {
        // the agents' tunnels: not here

        // wait for all participants to exit
        self.router.drain();
        // Here the original waits, unless `force`, for the room manager to have no
        // participants, looking every five seconds and logging "waiting for participants to
        // exit": with the room manager.
        let _ = force;

        if !self.running.swap(false) {
            return;
        }

        self.router.stop();
        self.done_chan.cancel();

        // wait for fully closed
        self.closed_chan.cancelled().await;
    }
}

// The deferred `UnregisterNode` of `Start`.
struct UnregisterNode<'a>(&'a dyn Router);

impl Drop for UnregisterNode<'_> {
    fn drop(&mut self) {
        if let Err(err) = self.0.unregister_node() {
            errorw!(logger::get_logger(), "could not unregister node", err);
        }
    }
}

// `net.Listen("tcp", net.JoinHostPort(addr, port))`.
async fn listen(addr: &str, port: u32) -> io::Result<tokio::net::TcpListener> {
    if addr.is_empty() {
        return match tokio::net::TcpListener::bind(("::", port as u16)).await {
            Ok(ln) => Ok(ln),
            Err(_) => tokio::net::TcpListener::bind(("0.0.0.0", port as u16)).await,
        };
    }
    if addr.contains(':') {
        return tokio::net::TcpListener::bind(format!("[{addr}]:{port}")).await;
    }
    tokio::net::TcpListener::bind(format!("{addr}:{port}")).await
}

pub async fn default_handler(State(s): State<ServerState>, uri: Uri) -> Response {
    if uri.path() == "/" {
        health_check(&s)
    } else {
        // http.NotFound
        (StatusCode::NOT_FOUND, "404 page not found\n").into_response()
    }
}

fn health_check(s: &ServerState) -> Response {
    let mut updated_at = None;
    if let Some(stats) = s.current_node.clone_node().stats {
        updated_at = Some(Time::unix(stats.updated_at, 0));
    }
    // a zero time.Time is long ago
    let since = updated_at.map_or(Duration(i64::MAX), time::since);
    if since > HEALTH_CHECK_MAX_AGE {
        return (
            StatusCode::NOT_ACCEPTABLE,
            format!("Not Ready\nNode Updated At {}", go_time_string(updated_at)),
        )
            .into_response();
    }

    (StatusCode::OK, "OK").into_response()
}

// Go's `time.Time.String()` of a time of whole seconds, in UTC; `None` is Go's zero time.
fn go_time_string(t: Option<Time>) -> String {
    let Some(t) = t else {
        return "0001-01-01 00:00:00 +0000 UTC".to_string();
    };
    let secs = t.unix_secs();
    let (days, rem) = (secs.div_euclid(86400), secs.rem_euclid(86400));
    // the civil date of a day since 1970-01-01 (Howard Hinnant's days_from_civil, inverted)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02} +0000 UTC",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// NewHTTPHandler builds the node's public HTTP handler. Of the original's API chain it has
/// the request body limit and the API key authentication (when there is a key provider),
/// and the path normalisation over all of it; the agent endpoint's chain is left out with
/// agents. The peer's address is put into each request as musivum's `RemoteAddr` (Go's
/// `r.RemoteAddr`), for a server that serves this with `ConnectInfo<SocketAddr>`.
pub fn new_http_handler(
    conf: &Config,
    key_provider: Option<Arc<dyn KeyProvider>>,
    api_handler: axum::Router,
) -> axum::Router {
    // negroni.NewRecovery() and CORS: not here (see the header)
    let mut api = api_handler;
    if let Some(key_provider) = key_provider {
        let auth_middleware = Arc::new(ApiKeyAuthMiddleware::new(key_provider));
        api = api.layer(from_fn(move |r: Request, next: Next| {
            api_key_auth(auth_middleware.clone(), r, next)
        }));
    }
    // limit request body size so large messages cannot exhaust memory
    let limiter = RequestBodyLimiter::new(conf.limit.max_api_request_body_size);
    api = api.layer(from_fn(move |r: Request, next: Next| {
        limiter.serve_http(r, next)
    }));

    with_path_normalization(api).layer(from_fn(remote_addr))
}

// `NewAPIKeyAuthMiddleware(keyProvider)` as a middleware of axum: musivum's function says
// whether the request goes on (with its grants) or what to answer.
async fn api_key_auth(
    auth_middleware: Arc<ApiKeyAuthMiddleware>,
    r: Request,
    next: Next,
) -> Response {
    let (mut parts, body) = r.into_parts();
    let mut headers = HeaderMap::new();
    let mut res = match auth_middleware.serve_http(&mut headers, &mut parts) {
        Ok(()) => next.run(Request::from_parts(parts, body)).await,
        Err(err) => (err.status, err.body).into_response(),
    };
    // what the middleware set on the response, unless the handler set it too
    for (name, value) in headers {
        if let Some(name) = name
            && !res.headers().contains_key(&name)
        {
            res.headers_mut().insert(name, value);
        }
    }
    res
}

// Go's `r.RemoteAddr`, from what axum's server knows of the connection.
async fn remote_addr(mut r: Request, next: Next) -> Response {
    if let Some(ConnectInfo(addr)) = r.extensions().get::<ConnectInfo<SocketAddr>>().copied() {
        r.extensions_mut().insert(RemoteAddr(addr));
    }
    next.run(r).await
}

#[cfg(test)]
#[path = "server_test.rs"]
mod server_test;
