// Ported from LiveKit, pkg/service/server.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, on tokio and axum, for one node.
// - The services are not injected one by one (`NewLivekitServer` takes eighteen). What the
//   routes share is `ServerState`; the routes are made by `new_mux` (the original's `mux`,
//   built inside `NewLivekitServer`), with the room API under Twirp's prefix and the
//   signalling endpoint, and handed to
//   `LivekitServer::new`. `initialize_server` stands in for wire's `InitializeServer`
//   (wire_gen.go) for what is ported: the bus of the process, the signal and room manager
//   clients, the router of one node (with the defaults of the node's statistics, as
//   `musivum::config` does not read `node_stats`), the local object store (`createStore`
//   without Redis), the room allocator, the key provider (`createKeyProvider` of wire.go),
//   the webhook notifier and the telemetry service (`createWebhookNotifier`,
//   `createTelemetryService`; the analytics service is the null one, LiveKit's is not
//   ported), the signalling endpoint, the room manager (with `createForwardStats`) and the
//   signal server. The WebRTC configuration (`rtc.NewWebRTCConfig`, which the original's
//   room manager makes) is made here, once, with the node's sockets, and handed to the room
//   manager, which closes the sockets when it stops.
// - The signalling endpoint's routes (`rtcService.SetupRoutes(mux)`) are axum handlers over
//   `RtcService`: the grants are in the request, put there by the API key middleware around
//   the routes, which also adds its headers to the response; the upgrade is axum's, with the
//   read limit `RtcUpgrade` gives, and the session runs on the socket as an
//   `AxumSignalWebSocket`.
// - Not here, with what they belong to: the Twirp servers of egress, ingress, SIP and agent
//   dispatch, WHIP, the agent endpoints and their tunnels (`drainAgentTunnels`), the IO info
//   service, WebTransport, the listener for debugging, and the debug handlers of
//   development mode
//   (`debugGoroutines`, which is Go's runtime, and `debugInfo`, which reads the room
//   manager). Of `NewHTTPHandler`'s middleware, negroni's recovery and the CORS handler of
//   rs/cors are not here (a handler that panics ends its connection; a browser on another
//   origin cannot read the answers); the request body limit, the API key authentication and
//   the path normalisation are.
// - `Start` and `Stop` are async. The done channel is made by `new` (a cancelled token cannot
//   be made anew, and the server is started once). `httpServer.Shutdown` with its timeout of
//   five seconds is the graceful shutdown of axum's server with that timeout: no new
//   connections, the requests in progress are answered; a WebSocket, once upgraded, is not
//   waited for, as Go's `Shutdown` does not wait for a hijacked connection. A server bound to
//   no address in particular listens on `[::]` (with IPv4 on the same socket, as Linux gives
//   it), or on `0.0.0.0` where there is no IPv6, where Go listens on both. `local_addrs` is
//   new: the addresses listened on, for a port of 0. `backgroundWorker` is a task.
// - The health check gives the time of the node's last update in UTC; the original gives it
//   in the server's time zone.
// - The embedded TURN server (wire's `newInProcessTurnServer`) is made by `start`, in the
//   runtime its sockets need (`initialize_server` runs before there is one), and closed where
//   `Stop` closes it; its auth handler is made for the room manager by `new_server_state`.
// - Go's `ServeMux` redirects a path with `..` or a doubled slash inside it to its clean
//   form; axum's router does not, and such a path is not found.
// - The Prometheus listener (`promServer`) is `PromServer` of promserver.rs, made, started
//   and stopped here where the original does it; it is stopped with the HTTP server.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{ConnectInfo, FromRequestParts, Request, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::middleware::{Next, from_fn};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use musivum::config::Config;
use musivum::go::atomic;
use musivum::go::time::{self, Duration, Time};
use musivum::mediatransportutil::rtcconfig;
use musivum::protocol::auth::{FileBasedKeyProvider, KeyProvider};
use musivum::protocol::utils::new_default_timed_version_generator;
use musivum::protocol::{logger, webhook};
use musivum::routing::{self, LocalNode, MessageBus, Router};
use musivum::service::{
    self as mservice, ApiKeyAuthMiddleware, LocalStore, RemoteAddr, RoomManager, RoomService,
    RoomServiceServer, RtcService, RtcUpgrade, SignalServer, livekit_server_options,
    new_room_allocator, new_room_service_server,
};
use musivum::sfu;
use musivum::telemetry::{self, AnalyticsService, NullAnalyticService, TelemetryService};
use musivum::webrtc::peerconnection::Node;
use musivum::webrtc::turn;
use musivum::{errorw, infow, rtc};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::axumws::{AxumSignalWebSocket, set_read_limit};
use super::promserver::PromServer;
use super::utils::{RequestBodyLimiter, with_path_normalization};

// the time `httpServer.Shutdown` is given
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
// how old the node's statistics may be for the node to be ready
const HEALTH_CHECK_MAX_AGE: Duration = Duration::from_secs(4);
// how often `Stop` looks whether the participants have left
const PART_TICKER_INTERVAL: Duration = Duration::from_secs(5);
// how often `backgroundWorker` closes the idle rooms
const ROOM_TICKER_INTERVAL: Duration = Duration::from_secs(1);

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("already running")]
    AlreadyRunning,
    #[error("one of key-file or keys must be provided in order to support a secure installation")]
    KeysNotProvided,
    #[error(
        "prometheus username or password is set but not both, set both or nothing for unauthenticated access"
    )]
    PrometheusUsernameOrPassword,
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Routing(#[from] routing::Error),
    #[error(transparent)]
    RtcConfig(#[from] rtcconfig::Error),
    #[error(transparent)]
    Service(#[from] mservice::Error),
    #[error(transparent)]
    Webhook(#[from] webhook::Error),
}

/// What the routes of the node share. The signalling endpoint and the room API take what
/// they need from here.
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
    pub rtc_service: Arc<RtcService>,
    pub room_manager: Arc<RoomManager>,
    pub signal_server: Arc<SignalServer>,
    /// The room API: roomservice.go behind the Twirp server that `NewRoomServiceServer`
    /// makes (`xtwirp.RegisterServer`).
    pub room_service_server: Arc<RoomServiceServer>,
    pub turn_auth_handler: Arc<mservice::TurnAuthHandler>,
}

/// wire's `InitializeServer`, for what is ported (see the header): the router of one node and
/// what it is made with, the services, the routes. It blocks (the WebRTC configuration may ask
/// STUN servers for the node's address): call it before the runtime or from `spawn_blocking`.
pub fn initialize_server(
    conf: Config,
    current_node: Arc<dyn LocalNode>,
) -> Result<Arc<LivekitServer>, Error> {
    let webrtc_config = rtc::WebRtcConfig::new(&conf)?;
    let rtc_node = webrtc_config.web_rtc_config.new_node()?;
    let state = new_server_state(conf, current_node, Arc::new(webrtc_config), rtc_node)?;
    let mux = new_mux(state.clone());
    LivekitServer::new(state, mux)
}

/// What `initialize_server` makes after the WebRTC configuration and the node's sockets, in
/// the order of wire's `InitializeServer`.
pub fn new_server_state(
    conf: Config,
    current_node: Arc<dyn LocalNode>,
    webrtc_config: Arc<rtc::WebRtcConfig>,
    rtc_node: Node,
) -> Result<ServerState, Error> {
    let conf = Arc::new(conf);
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
    // createStore without Redis
    let object_store = LocalStore::new();
    let room_allocator = new_room_allocator(conf.clone(), object_store.clone());
    // NewRoomService, with the store as its OSS service store, and the room and participant
    // clients of the bus
    let room_service = RoomService::new(
        conf.limit.clone(),
        conf.api,
        router.clone(),
        room_allocator.clone(),
        object_store.clone(),
        Some(object_store.clone()),
        routing::new_topic_formatter(),
        routing::new_typed_room_client(message_bus.clone())?,
        routing::new_typed_participant_client(message_bus.clone())?,
    )?;
    let room_service_server = Arc::new(new_room_service_server(
        room_service,
        livekit_server_options(),
    ));
    let key_provider = create_key_provider(&conf)?;
    let queued_notifier = create_webhook_notifier(&conf, key_provider.clone())?;
    // telemetry.NewAnalyticsService: not ported (see the header)
    let analytics_service: Arc<dyn AnalyticsService> = Arc::new(NullAnalyticService {});
    let telemetry_service = create_telemetry_service(Some(queued_notifier), analytics_service);
    let rtc_service = RtcService::new(
        conf.clone(),
        room_allocator.clone(),
        router.clone(),
        telemetry_service.clone(),
    );
    let timed_version_generator = new_default_timed_version_generator();
    let forward_stats = create_forward_stats(&conf);
    let turn_auth_handler = mservice::new_turn_auth_handler(key_provider.clone());
    let room_manager = RoomManager::new_local(
        conf.clone(),
        object_store,
        current_node.clone(),
        router.clone(),
        room_allocator,
        telemetry_service,
        timed_version_generator,
        Some(turn_auth_handler.clone()),
        message_bus.clone(),
        forward_stats,
        webrtc_config.clone(),
        rtc_node.clone(),
    )?;
    let signal_server = SignalServer::new_default(
        current_node.clone(),
        message_bus.clone(),
        router.clone(),
        room_manager.clone(),
    )?;
    Ok(ServerState {
        config: conf,
        current_node,
        message_bus,
        router,
        key_provider,
        webrtc_config,
        rtc_node,
        rtc_service,
        room_manager,
        signal_server: Arc::new(signal_server),
        room_service_server,
        turn_auth_handler,
    })
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

// createWebhookNotifier of wire.go
fn create_webhook_notifier(
    conf: &Config,
    provider: Arc<dyn KeyProvider>,
) -> Result<Arc<dyn webhook::QueuedNotifier>, Error> {
    let wc = &conf.web_hook;

    let secret = provider.get_secret(&wc.api_key);
    if secret.is_empty() && !wc.urls.is_empty() {
        return Err(mservice::Error::WebHookMissingApiKey.into());
    }

    Ok(webhook::new_default_notifier(wc, provider)?)
}

// createTelemetryService of wire.go. The hook holds the service weakly: the service holds the
// notifier.
fn create_telemetry_service(
    notifier: Option<Arc<dyn webhook::QueuedNotifier>>,
    analytics: Arc<dyn AnalyticsService>,
) -> Arc<dyn TelemetryService> {
    let svc = telemetry::new_telemetry_service(notifier.clone(), analytics);
    if let Some(notifier) = notifier {
        let weak = Arc::downgrade(&svc);
        notifier.register_processed_hook(Arc::new(move |info| {
            if let Some(svc) = weak.upgrade() {
                svc.webhook(info);
            }
        }));
    }
    svc
}

// createForwardStats of wire.go
fn create_forward_stats(conf: &Config) -> Option<Arc<sfu::ForwardStats>> {
    let fs = &conf.rtc.forward_stats;
    if fs.summary_interval == Duration::ZERO
        || fs.report_interval == Duration::ZERO
        || fs.report_window == Duration::ZERO
    {
        return None;
    }
    Some(sfu::ForwardStats::new(
        fs.summary_interval,
        fs.report_interval,
        fs.report_window,
    ))
}

/// The routes of the node: the original's `mux`. Everything that is not routed to anything
/// else reaches `default_handler`, which answers the health check on `/`.
pub fn new_mux(state: ServerState) -> axum::Router {
    // In development mode the original adds pprof, /debug/goroutine and /debug/rooms: not
    // here (see the header).
    axum::Router::new()
        // `xtwirp.RegisterServer(mux, roomServer)`: the room API over Twirp, every method under
        // its path prefix. Egress, ingress, SIP, agent dispatch, WHIP and `/agent` are left out.
        .route(
            &format!("{}{{*method}}", mservice::ROOM_SERVICE_PATH_PREFIX),
            any(room_service),
        )
        // rtcService.SetupRoutes(mux)
        .route("/rtc", any(rtc_v0))
        .route("/rtc/validate", any(rtc_v0_validate))
        .route("/rtc/v1", any(rtc_v1))
        .route("/rtc/v1/validate", any(rtc_v1_validate))
        .fallback(default_handler)
        .with_state(state)
}

// The room API: the Twirp server takes the request's parts, with the grants the API key
// middleware put there, and its body. A call may wait (`PerformRpc`, `UpdateRoomMetadata`
// wait for the room), so it runs where blocking is allowed.
async fn room_service(State(s): State<ServerState>, r: Request) -> Response {
    let (parts, body) = r.into_parts();
    let Ok(body) = axum::body::to_bytes(body, usize::MAX).await else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let server = s.room_service_server.clone();
    match tokio::task::spawn_blocking(move || server.serve_http(&parts, &body)).await {
        Ok(res) => res.map(axum::body::Body::from),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

// The handlers of `/rtc` and `/rtc/v1`: `RtcService::v0` and `v1`, and the session on the
// upgraded socket. The API key middleware has run: the grants are in the request.
async fn rtc_v0(State(s): State<ServerState>, r: Request) -> Response {
    let (mut parts, _) = r.into_parts();
    let ws = WebSocketUpgrade::from_request_parts(&mut parts, &()).await;
    let res = s.rtc_service.v0(&parts).await;
    rtc_upgrade(ws, res)
}

async fn rtc_v1(State(s): State<ServerState>, r: Request) -> Response {
    let (mut parts, _) = r.into_parts();
    let ws = WebSocketUpgrade::from_request_parts(&mut parts, &()).await;
    let res = s.rtc_service.v1(&parts).await;
    rtc_upgrade(ws, res)
}

async fn rtc_v0_validate(State(s): State<ServerState>, r: Request) -> Response {
    let (parts, _) = r.into_parts();
    s.rtc_service
        .v0_validate(&parts)
        .map(axum::body::Body::from)
}

async fn rtc_v1_validate(State(s): State<ServerState>, r: Request) -> Response {
    let (parts, _) = r.into_parts();
    s.rtc_service
        .v1_validate(&parts)
        .map(axum::body::Body::from)
}

// `upgrader.Upgrade` and the rest of `serve`: the response `RtcService` wrote, or the upgrade
// run on the socket. `RtcService` has checked the request as gorilla's upgrader does; should
// axum refuse it all the same, its answer is written and the upgrade dropped, which ends the
// session as a failed upgrade does upstream.
#[allow(clippy::result_large_err)]
fn rtc_upgrade(
    ws: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
    res: Result<RtcUpgrade, axum::http::Response<bytes::Bytes>>,
) -> Response {
    let upgrade = match res {
        Ok(upgrade) => upgrade,
        Err(res) => return res.map(axum::body::Body::from),
    };
    let ws = match ws {
        Ok(ws) => ws,
        Err(rejection) => return rejection.into_response(),
    };
    let limit = upgrade.read_limit();
    set_read_limit(ws, limit).on_upgrade(move |socket| async move {
        upgrade.run(AxumSignalWebSocket::new(socket)).await;
    })
}

pub struct LivekitServer {
    config: Arc<Config>,
    state: ServerState,
    http_server: axum::Router,
    prom_server: Option<PromServer>,
    router: Arc<dyn Router>,
    room_manager: Arc<RoomManager>,
    signal_server: Arc<SignalServer>,
    current_node: Arc<dyn LocalNode>,
    turn_server: Mutex<Option<Arc<turn::Server>>>,
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

        let prom_server = PromServer::new(&conf, state.current_node.as_ref())?;

        // debug_handler and webtransport are taken by the configuration and not read (see the
        // header).

        state.router.remove_dead_nodes()?;

        Ok(Arc::new(LivekitServer {
            config: conf,
            current_node: state.current_node.clone(),
            router: state.router.clone(),
            room_manager: state.room_manager.clone(),
            signal_server: state.signal_server.clone(),
            state,
            http_server,
            prom_server,
            turn_server: Mutex::new(None),
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
        let mut prom_listeners = Vec::new();
        for addr in &addresses {
            let ln = listen(addr, self.config.port).await?;
            self.local_addrs.lock().push(ln.local_addr()?);
            listeners.push(ln);

            if let Some(prom_server) = &self.prom_server {
                prom_listeners.push(prom_server.listen(addr).await?);
            }

            // the listener for debugging: not here
        }

        // WebTransport: not here

        // newInProcessTurnServer (see the header)
        *self.turn_server.lock() = mservice::new_turn_server(
            &self.config,
            Some(mservice::get_turn_auth_handler_func(
                &self.state.turn_auth_handler,
            )),
            false,
        )?;

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
            let prom_port = self.prom_server.as_ref().map(PromServer::port);
            if let Some(prom_port) = &prom_port {
                values.push(("portPrometheus", prom_port));
            }
            if !self.config.region.is_empty() {
                values.push(("region", &self.config.region));
            }
            logger::get_logger().infow("starting LiveKit server", &values);
        }

        if let Some(prom_server) = &self.prom_server {
            prom_server.serve(prom_listeners);
        }

        self.signal_server.start()?;

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

        {
            let s = self.clone();
            time::spawn(async move { s.background_worker().await });
        }

        // give time for Serve goroutine to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        self.running.store(true);

        self.done_chan.cancelled().await;

        // the agents' tunnels: not here

        // wait for shutdown
        shutdown.cancel();
        let _ = tokio::time::timeout(SHUTDOWN_TIMEOUT.to_std(), http_closed.cancelled()).await;
        if let Some(prom_server) = &self.prom_server {
            prom_server.shutdown();
        }

        // the debug server and WebTransport: not here

        if let Some(turn_server) = self.turn_server.lock().take() {
            let _ = turn_server.close();
        }

        self.room_manager.stop();
        self.signal_server.stop();
        // the IO info service: not here

        self.closed_chan.cancel();
        Ok(())
    }

    pub async fn stop(&self, force: bool) {
        // the agents' tunnels: not here

        // wait for all participants to exit
        self.router.drain();
        let mut part_ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + PART_TICKER_INTERVAL.to_std(),
            PART_TICKER_INTERVAL.to_std(),
        );
        let mut waiting_for_participants = !force && self.room_manager.has_participants();
        while waiting_for_participants {
            part_ticker.tick().await;
            infow!(logger::get_logger(), "waiting for participants to exit");
            waiting_for_participants = self.room_manager.has_participants();
        }

        if !self.running.swap(false) {
            return;
        }

        self.router.stop();
        self.done_chan.cancel();

        // wait for fully closed
        self.closed_chan.cancelled().await;
    }

    pub fn room_manager(&self) -> Arc<RoomManager> {
        self.room_manager.clone()
    }

    async fn background_worker(&self) {
        let mut room_ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + ROOM_TICKER_INTERVAL.to_std(),
            ROOM_TICKER_INTERVAL.to_std(),
        );
        // a ticker of Go drops the ticks nobody takes
        room_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = self.done_chan.cancelled() => return,
                _ = room_ticker.tick() => self.room_manager.close_idle_rooms(),
            }
        }
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
pub(super) async fn listen(addr: &str, port: u32) -> io::Result<tokio::net::TcpListener> {
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
