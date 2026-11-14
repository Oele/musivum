//! The HTTP side of LiveKit's `pkg/service` for one node, on axum: the server (`server.rs`,
//! from `server.go`), the middleware of `utils.go` that `musivum::service` leaves to a server
//! (`utils.rs`), and axum's WebSocket for the signalling (`axumws.rs`, written new). The rest
//! of the package is in `musivum::service`.

mod axumws;
mod server;
mod utils;

pub use axumws::*;
pub use server::*;
pub use utils::{RequestBodyLimiter, remove_double_slashes, with_path_normalization};

#[cfg(test)]
#[path = "requestlimit_test.rs"]
mod requestlimit_test;
