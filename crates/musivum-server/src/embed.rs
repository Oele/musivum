//! musivum inside another server's process: everything `musivum-server` starts but its HTTP
//! listener, with the routes as an axum router for that server to serve, the room API as
//! calls, the webhook events in process, and a shutdown.
//!
//! Written for a Matrix homeserver on axum 0.8 and tokio 1 that serves LiveKit's signalling
//! under a path of its own and makes the room API calls and receives the webhooks a server
//! makes of a LiveKit SFU. It shares the wiring with `musivum-server` (`service::server`):
//! [`Embedded::start`] makes what `initialize_server` makes and runs what
//! `LivekitServer::start` runs, without binding the HTTP port.
//!
//! # Starting
//!
//! From the same YAML that is written for `livekit-server` (`port` is not read, there is no
//! listener of musivum's own):
//!
//! ```no_run
//! # async fn f(yaml: &str) -> Result<(), Box<dyn std::error::Error>> {
//! use musivum_server::embed::{EmbedOptions, Embedded};
//!
//! // `Config::new` blocks when the node's address is asked of STUN servers
//! // (`rtc.use_external_ip`): call it where blocking is allowed.
//! let conf = musivum::config::Config::new(yaml, true)?;
//! let embedded = Embedded::start(
//!     conf,
//!     EmbedOptions {
//!         // the port clients reach TURN over TLS at (LiveKit names 443 whatever
//!         // `turn.tls_port` is)
//!         advertised_turn_tls_port: Some(5349),
//!         ..Default::default()
//!     },
//! )
//! .await?;
//! # Ok(()) }
//! ```
//!
//! [`Embedded::start`] is async and runs on the caller's tokio runtime, which must be a
//! multi-threaded one (musivum blocks a worker in places, with `block_in_place`). It does what
//! `musivum-server` does before it serves: it checks the keys (`Config::validate_keys`) and
//! reads the secrets of `rtc.turn_servers` (`Config::load_turn_secrets`), makes the node and
//! the WebRTC configuration (which may ask STUN servers for the node's address) and binds the
//! media sockets (UDP, ICE over TCP), all in `spawn_blocking`; then it starts the router, the
//! TURN server, the signal server and the worker that closes idle rooms, and with
//! [`EmbedOptions::metrics`] the Prometheus listener of `prometheus.port`. It returns once all
//! of that runs, or the error that stopped it (a port that is taken), having closed what it
//! had opened.
//!
//! # Mounting the routes
//!
//! [`Embedded::router`] is the HTTP side of the node, with its state applied: `/rtc`,
//! `/rtc/validate`, `/rtc/v1`, `/rtc/v1/validate` (the WebSocket upgrade included) and the
//! room API under `/twirp/livekit.RoomService/`, behind the middleware `musivum-server` lays
//! over them (the API key authentication, the bound on a request's body, the path
//! normalisation). `/` answers the health check, and any other path 404. Nest it under a
//! prefix: axum strips the prefix before musivum sees the path, so the routes are the same
//! below it. Serve the application with `ConnectInfo<SocketAddr>`, from which musivum takes the
//! client's address where no `CF-Connecting-IP`, `X-Forwarded-For` or `X-Real-IP` header
//! names one (as LiveKit does, these headers are trusted):
//!
//! ```no_run
//! # async fn f(embedded: musivum_server::embed::Embedded) -> std::io::Result<()> {
//! use std::net::SocketAddr;
//!
//! let app = axum::Router::new().nest(
//!     "/_matrix/client/unstable/io.element.msc4195/rtc/livekit/signal",
//!     embedded.router(),
//! );
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:8008").await?;
//! axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await
//! # }
//! ```
//!
//! A WebSocket session runs as a task of the server that serves the route, on its runtime.
//! From the end of the participants' wait in [`Embedded::shutdown`] on, the router answers
//! 503 and holds nothing of musivum.
//!
//! # Calling the room API
//!
//! [`Embedded::create_room`], [`Embedded::list_participants`] and
//! [`Embedded::remove_participant`] take the room API's requests and give its responses,
//! through the same room service, router and room manager as a call over HTTP, with the
//! grants of an administrator of the room the request names and without a token. An error is
//! the room service's; [`twirp_code`] gives the code a call over HTTP would have answered with
//! (`"not_found"` for a room or participant that is not there). The rest of the room API is
//! [`ServerState::room_service`] (`embedded.state().room_service`), called with
//! [`call_context`] from where blocking is allowed (`spawn_blocking`), as the HTTP route calls
//! it.
//!
//! ```no_run
//! # async fn f(embedded: &musivum_server::embed::Embedded) -> Result<(), musivum::service::Error> {
//! use musivum::protocol::livekit;
//!
//! embedded
//!     .create_room(livekit::CreateRoomRequest {
//!         name: "a-room".to_string(),
//!         empty_timeout: 300,
//!         departure_timeout: 20,
//!         ..Default::default()
//!     })
//!     .await?;
//! let participants = embedded
//!     .list_participants(livekit::ListParticipantsRequest {
//!         room: "a-room".to_string(),
//!     })
//!     .await?
//!     .participants;
//! # Ok(()) }
//! ```
//!
//! # Receiving the webhook events
//!
//! [`Embedded::events`] is a receiver of every webhook event, the same `WebhookEvent` (with
//! its `id` and `created_at`) the webhooks of `webhook.urls` carry, after the same filter
//! (`webhook.filter_params`). It gets the events that happen after it was made. The HTTP
//! webhooks of the configuration are sent as well; leave `webhook.urls` empty to have none.
//! The receiver is a `tokio::sync::broadcast` one: a receiver that falls more than
//! [`EmbedOptions::event_capacity`] events behind loses the oldest and is told how many
//! (`RecvError::Lagged`), as a webhook queue drops what waits too long. After
//! [`Embedded::shutdown`], once the events of the shutdown (the participants that left, the
//! rooms that closed) are handed on and musivum has let go of its services, a receiver gets
//! `RecvError::Closed`; the HTTP webhooks still queued are sent then.
//!
//! ```no_run
//! # fn f(embedded: &musivum_server::embed::Embedded) {
//! use tokio::sync::broadcast::error::RecvError;
//!
//! let mut events = embedded.events();
//! tokio::spawn(async move {
//!     loop {
//!         match events.recv().await {
//!             Ok(event) => match event.event.as_str() {
//!                 "participant_joined" | "participant_left" | "room_finished" => { /* ... */ }
//!                 _ => {}
//!             },
//!             Err(RecvError::Lagged(n)) => { /* n events lost */ }
//!             Err(RecvError::Closed) => break,
//!         }
//!     }
//! });
//! # }
//! ```
//!
//! # Shutting down
//!
//! [`Embedded::shutdown`] is `LivekitServer::stop`: the node is drained (no new
//! participants), the participants are given until the timeout to leave (looked at every
//! 100 ms, where the server looks every 5 s), and then the routes answer no more and whoever
//! is left is closed; the TURN server, the media sockets and the Prometheus listener are
//! closed, the background workers end. It returns when that is done, the media sockets
//! included: their ports can be bound again (a new [`Embedded::start`]). The WebSocket
//! sessions end with their participants. Stop serving [`Embedded::router`] before or after,
//! as the embedding server shuts down. Dropping an `Embedded` without shutting it down leaves
//! musivum running.
//!
//! # Threads
//!
//! On the caller's runtime: the routes and the WebSocket sessions (on the runtime of the
//! server that serves them), what [`Embedded::start`] runs (the idle-room worker, the router's
//! statistics, the TURN server, the Prometheus listener), the webhook senders, and the tasks
//! musivum starts from those (a task of musivum runs on the runtime of whoever starts it). Of
//! its own, musivum has:
//!
//! - `musivum-net`: a tokio runtime with a thread per processor that reads the media sockets
//!   (UDP and ICE over TCP) of every peer connection, and runs what that reading starts. It
//!   is made with the first media socket and stays to the end of the process, idle once the
//!   sockets are closed.
//! - `musivum-worker`: up to a thread per processor, made as packets are first fanned out to
//!   many subscribers; they sleep when there is nothing to do. rayon's global pool (a thread
//!   per processor, made at first use) runs the other work that LiveKit does on many
//!   goroutines at once.
//! - `opsqueue <name>`: a thread per queue of operations (the telemetry's, one per room and
//!   per participant), each in the runtime of whoever made it, ending with its queue.
//! - `musivum-timer`: two threads, made only if a timer or task of musivum is started from a
//!   thread that is on no runtime.
//! - Short-lived threads: those that close participants and stop the webhook queues, and
//!   `spawn_blocking` for the room API calls, as the HTTP route makes them.
//!
//! # Logging
//!
//! musivum logs through `tracing`, with the target `musivum`, to whatever subscriber the
//! process has. The `logging` section of the configuration is not applied (that is
//! `musivum-server`'s `logger`).
//!
//! # Panics
//!
//! Where LiveKit's code would end the process, musivum panics. With panics that unwind, that
//! ends the task or thread it happens in, not the process.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::Request;
use axum::http::{Extensions, StatusCode};
use axum::response::IntoResponse;
use musivum::config::Config;
use musivum::infow;
use musivum::protocol::auth::{ClaimGrants, VideoGrant};
use musivum::protocol::livekit;
use musivum::protocol::logger;
use musivum::protocol::webhook::{self, FilterParams, NotifyOption, QueuedNotifier};
use musivum::routing::{LocalNode, LocalNodeImpl};
use musivum::service::{self as mservice, RoomService};
use parking_lot::{Mutex, RwLock};
use tokio::sync::broadcast;
use tower::ServiceExt;

use crate::service::{
    self, LivekitServer, ServerState, initialize_server_state, new_http_handler, new_mux,
};

/// How often [`Embedded::shutdown`] looks whether the participants have left.
const PARTICIPANT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// What an embedding server chooses that `musivum-server` takes from its command line or
/// does not have.
#[derive(Clone, Debug)]
pub struct EmbedOptions {
    /// The port the `turns:` URL of the ICE servers names, in the join and reconnect
    /// responses. `None` names 443, whatever `turn.tls_port` is, as LiveKit does (it expects
    /// a load balancer on 443); `Some(port)` names that port, for instance `turn.tls_port`
    /// where clients reach it directly. Nothing is advertised without `turn.tls_port`.
    pub advertised_turn_tls_port: Option<u16>,
    /// Whether the Prometheus listener of `prometheus.port` is started. Off by default: an
    /// embedding server has metrics of its own.
    pub metrics: bool,
    /// How many events a receiver of [`Embedded::events`] may fall behind before it loses the
    /// oldest.
    pub event_capacity: usize,
}

impl Default for EmbedOptions {
    fn default() -> EmbedOptions {
        EmbedOptions {
            advertised_turn_tls_port: None,
            metrics: false,
            event_capacity: 1024,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Config(#[from] musivum::config::Error),
    #[error(transparent)]
    Routing(#[from] musivum::routing::Error),
    #[error(transparent)]
    Server(#[from] service::Error),
    /// The work of starting ended without an answer (it panicked).
    #[error("starting musivum failed: {0}")]
    Start(String),
}

/// musivum running in the caller's process: see the module's description.
pub struct Embedded {
    state: ServerState,
    server: Arc<LivekitServer>,
    // `LivekitServer::start`, which returns when the server has stopped
    serving: tokio::task::JoinHandle<Result<(), service::Error>>,
    events: Arc<EventNotifier>,
    // What every `router` serves: the routes and middleware of `musivum-server`, until the
    // shutdown takes them away (as `Stop` shuts the HTTP server down) and with them what
    // they hold of musivum.
    routes: Arc<RwLock<Option<axum::Router>>>,
}

impl Embedded {
    /// Starts everything `musivum-server` starts but its HTTP listener, on the caller's
    /// runtime: see the module's description.
    pub async fn start(mut conf: Config, opts: EmbedOptions) -> Result<Embedded, Error> {
        let events = EventNotifier::new(
            conf.web_hook.filter_params.clone(),
            opts.event_capacity.max(1),
        );
        let notifier: Arc<dyn QueuedNotifier> = events.clone();
        let made = tokio::task::spawn_blocking(move || -> Result<_, Error> {
            // what `startServer` of main.rs does with the configuration
            conf.validate_keys()?;
            conf.load_turn_secrets()?;
            let current_node: Arc<dyn LocalNode> = LocalNodeImpl::new(Some(&conf))?;
            Ok(initialize_server_state(conf, current_node, Some(notifier))?)
        })
        .await;
        let state = made.map_err(|err| Error::Start(err.to_string()))??;
        state
            .room_manager
            .set_advertised_turn_tls_port(opts.advertised_turn_tls_port);
        let server = match LivekitServer::new_embedded(state.clone(), opts.metrics) {
            Ok(server) => server,
            Err(err) => {
                close(&state).await;
                return Err(err.into());
            }
        };

        let mut serving = tokio::spawn({
            let server = server.clone();
            async move { server.start().await }
        });
        tokio::select! {
            res = &mut serving => {
                // `start` ended before it ran: it could not bind a port
                close(&state).await;
                return Err(match res {
                    Ok(Err(err)) => err.into(),
                    Ok(Ok(())) => Error::Start("stopped while starting".to_string()),
                    Err(err) => Error::Start(err.to_string()),
                });
            }
            _ = server.started() => {}
        }

        let routes = new_http_handler(
            &state.config,
            Some(state.key_provider.clone()),
            new_mux(state.clone()),
        );
        Ok(Embedded {
            state,
            server,
            serving,
            events,
            routes: Arc::new(RwLock::new(Some(routes))),
        })
    }

    /// The routes of the node, with their middleware and state, to be nested under a prefix
    /// and served with `ConnectInfo<SocketAddr>`: see the module's description. After
    /// [`Embedded::shutdown`] they answer 503.
    pub fn router(&self) -> axum::Router {
        let routes = self.routes.clone();
        axum::Router::new().fallback_service(tower::service_fn(move |r: Request| {
            let served = routes.read().clone();
            async move {
                Ok::<_, Infallible>(match served {
                    Some(served) => served.oneshot(r).await?,
                    None => {
                        (StatusCode::SERVICE_UNAVAILABLE, "musivum has stopped").into_response()
                    }
                })
            }
        }))
    }

    /// A receiver of the webhook events from now on: see the module's description.
    pub fn events(&self) -> broadcast::Receiver<livekit::WebhookEvent> {
        self.events.subscribe()
    }

    /// The room API's `CreateRoom`: the room as it is, made if it was not there, with the
    /// request's `empty_timeout` and `departure_timeout`.
    pub async fn create_room(
        &self,
        req: livekit::CreateRoomRequest,
    ) -> Result<livekit::Room, mservice::Error> {
        let ctx = call_context(&req.name);
        self.call(move |svc| svc.create_room(&ctx, &req)).await
    }

    /// The room API's `ListParticipants`. A room that is not there has none, with the
    /// default `api` section (the participants are read from the store).
    pub async fn list_participants(
        &self,
        req: livekit::ListParticipantsRequest,
    ) -> Result<livekit::ListParticipantsResponse, mservice::Error> {
        let ctx = call_context(&req.room);
        self.call(move |svc| svc.list_participants(&ctx, &req))
            .await
    }

    /// The room API's `RemoveParticipant`: an error with the code `not_found` for a
    /// participant that is not there.
    pub async fn remove_participant(
        &self,
        req: livekit::RoomParticipantIdentity,
    ) -> Result<livekit::RemoveParticipantResponse, mservice::Error> {
        let ctx = call_context(&req.room);
        self.call(move |svc| svc.remove_participant(&ctx, &req))
            .await
    }

    // A call of the room service, where blocking is allowed, as the HTTP route makes it.
    async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&RoomService) -> Result<T, mservice::Error> + Send + 'static,
    ) -> Result<T, mservice::Error> {
        let svc = self.state.room_service.clone();
        match tokio::task::spawn_blocking(move || f(&svc)).await {
            Ok(res) => res,
            // the HTTP route answers 500
            Err(_) => Err(mservice::Error::Twirp(mservice::internal_error(
                "the call ended without an answer",
            ))),
        }
    }

    /// What the routes and the calls share: the room manager, the router, the room service,
    /// the configuration.
    pub fn state(&self) -> &ServerState {
        &self.state
    }

    /// `LivekitServer::stop`: drains the node, waits until `timeout` for the participants to
    /// leave, then closes the rest and returns when everything is closed. See the module's
    /// description.
    pub async fn shutdown(self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;

        // `Stop(false)`: wait for all participants to exit, here until the deadline
        self.state.router.drain();
        if self.state.room_manager.has_participants() {
            infow!(logger::get_logger(), "waiting for participants to exit");
        }
        while self.state.room_manager.has_participants() {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }
            tokio::time::sleep((deadline - now).min(PARTICIPANT_POLL_INTERVAL)).await;
        }

        // `Stop(true)` for what is left: `start` shuts the HTTP server down (here: the routes
        // answer no more), closes everything and returns
        self.routes.write().take();
        self.server.stop(true).await;
        if let Ok(Err(err)) = self.serving.await {
            musivum::errorw!(logger::get_logger(), "musivum stopped with an error", err);
        }
        // the media sockets are closed when the tasks that read them have let go of them
        self.state.rtc_node.closed().await;
    }
}

/// The request context a direct call of the room service is made with: the grants of an
/// administrator of `room` who may also create and list rooms, without a token.
pub fn call_context(room: &str) -> Extensions {
    let mut ctx = Extensions::new();
    mservice::with_grants(
        &mut ctx,
        Some(ClaimGrants {
            video: Some(VideoGrant {
                room_create: true,
                room_list: true,
                room_admin: true,
                room: room.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        "",
    );
    ctx
}

/// The Twirp code a call over HTTP would have answered `err` with (`"not_found"`,
/// `"permission_denied"`, ...).
pub fn twirp_code(err: &mservice::Error) -> String {
    mservice::to_error(err).code().to_string()
}

// What `LivekitServer::start` closes, for a server that never ran: the media sockets (with
// the room manager) and the signal server's place on the bus.
async fn close(state: &ServerState) {
    state.room_manager.stop();
    state.signal_server.stop();
    state.rtc_node.closed().await;
}

/// The webhook events in process: a notifier beside those of the configuration, with their
/// filter, that hands every event it is given to the receivers of [`Embedded::events`].
struct EventNotifier {
    filter: RwLock<FilterParams>,
    // `None` once stopped
    sender: Mutex<Option<broadcast::Sender<livekit::WebhookEvent>>>,
}

impl EventNotifier {
    fn new(filter: FilterParams, capacity: usize) -> Arc<EventNotifier> {
        Arc::new(EventNotifier {
            filter: RwLock::new(filter),
            sender: Mutex::new(Some(broadcast::channel(capacity).0)),
        })
    }

    fn subscribe(&self) -> broadcast::Receiver<livekit::WebhookEvent> {
        match &*self.sender.lock() {
            Some(sender) => sender.subscribe(),
            // a receiver of a channel that is closed
            None => broadcast::channel(1).1,
        }
    }
}

impl QueuedNotifier for EventNotifier {
    // The hook is told of webhooks sent and dropped: nothing is sent here.
    fn register_processed_hook(&self, _f: Arc<dyn Fn(&livekit::WebhookInfo) + Send + Sync>) {}

    // Nothing is signed here.
    fn set_keys(&self, _api_key: &str, _api_secret: &str) {}

    fn set_filter(&self, params: FilterParams) {
        *self.filter.write() = params;
    }

    fn queue_notify(
        &self,
        event: &livekit::WebhookEvent,
        _opts: &[NotifyOption],
    ) -> Result<(), webhook::Error> {
        if !self.filter.read().is_allowed(&event.event) {
            return Ok(());
        }
        match &*self.sender.lock() {
            // no receiver is not an error: nobody listens
            Some(sender) => {
                let _ = sender.send(event.clone());
                Ok(())
            }
            None => Err(webhook::Error::Closed),
        }
    }

    fn stop(&self, _force: bool) {
        self.sender.lock().take();
    }
}

/// The notifiers the telemetry service is given: the one of the configuration and the
/// embedding server's.
pub(crate) fn new_notifiers(notifiers: Vec<Arc<dyn QueuedNotifier>>) -> Arc<dyn QueuedNotifier> {
    Arc::new(Notifiers(notifiers))
}

// Every call goes to each; an event goes to each even when one fails, and the first error
// is returned.
struct Notifiers(Vec<Arc<dyn QueuedNotifier>>);

impl QueuedNotifier for Notifiers {
    fn register_processed_hook(&self, f: Arc<dyn Fn(&livekit::WebhookInfo) + Send + Sync>) {
        for n in &self.0 {
            n.register_processed_hook(f.clone());
        }
    }

    fn set_keys(&self, api_key: &str, api_secret: &str) {
        for n in &self.0 {
            n.set_keys(api_key, api_secret);
        }
    }

    fn set_filter(&self, params: FilterParams) {
        for n in &self.0 {
            n.set_filter(params.clone());
        }
    }

    fn queue_notify(
        &self,
        event: &livekit::WebhookEvent,
        opts: &[NotifyOption],
    ) -> Result<(), webhook::Error> {
        let mut res = Ok(());
        for n in &self.0 {
            if let Err(err) = n.queue_notify(event, opts)
                && res.is_ok()
            {
                res = Err(err);
            }
        }
        res
    }

    fn stop(&self, force: bool) {
        for n in &self.0 {
            n.stop(force);
        }
    }
}

// The telemetry service holds the notifiers until its last queued job has run, the events of
// the shutdown among them: then what the webhook queues hold is sent and they end, and the
// receivers of the events are told that there will be none more.
impl Drop for Notifiers {
    fn drop(&mut self) {
        self.stop(false);
    }
}
