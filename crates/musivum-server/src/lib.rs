//! The server around musivum: what LiveKit's `cmd/server` and the HTTP side of its
//! `pkg/service` do for one node, on tokio and axum.
//!
//! The library (`musivum`) owns no HTTP server: its service layer works on the `http` crate's
//! request types and on a WebSocket behind a trait. This crate is a server for it, and the
//! binary `musivum-server` (`main.rs`, `commands.rs`: LiveKit's `cmd/server`) is that server
//! as a program. What a server that builds musivum in (a Matrix homeserver on axum) can take
//! from here:
//!
//! - [`embed`]: all of it in one handle. [`embed::Embedded::start`] starts what the server
//!   starts but its HTTP listener, on the caller's runtime; the routes are an axum router to
//!   nest, the room API is a few calls, the webhook events come in process, and
//!   [`embed::Embedded::shutdown`] stops it.
//! - [`service::AxumSignalWebSocket`]: axum's WebSocket as musivum's
//!   [`SignalWebSocket`](musivum::service::SignalWebSocket).
//! - [`service::initialize_server`] and [`service::new_server_state`]: the services of one
//!   node made and wired (the router, the room manager, the signalling endpoint).
//! - [`service::new_mux`]: the routes of the node: the signalling endpoint, and the room API
//!   when it is added; [`service::new_http_handler`]: the middleware LiveKit lays over them.
//! - [`service::LivekitServer`]: listening, the health check, and shutting down.
//! - [`logger`]: the log, set up from the configuration.

pub mod embed;
pub mod logger;
pub mod service;
