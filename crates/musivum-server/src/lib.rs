//! The server around musivum: what LiveKit's `cmd/server` and the HTTP side of its
//! `pkg/service` do for one node, on tokio and axum.
//!
//! The library (`musivum`) owns no HTTP server: its service layer works on the `http` crate's
//! request types and on a WebSocket behind a trait. This crate is a server for it, and the
//! binary `musivum-server` (`main.rs`, `commands.rs`: LiveKit's `cmd/server`) is that server
//! as a program. What a server that builds musivum in (a Matrix homeserver on axum) can take
//! from here:
//!
//! - [`service::AxumSignalWebSocket`]: axum's WebSocket as musivum's
//!   [`SignalWebSocket`](musivum::service::SignalWebSocket).
//! - [`service::new_mux`]: the routes of the node, where the signalling endpoint and the room
//!   API are added; [`service::new_http_handler`]: the middleware LiveKit lays over them.
//! - [`service::LivekitServer`]: listening, the health check, and shutting down.
//! - [`logger`]: the log, set up from the configuration.

pub mod logger;
pub mod service;
