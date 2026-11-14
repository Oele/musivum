//! LiveKit's `pkg/service`, as far as one node needs it: the outer layer that clients and
//! API callers talk to.
//!
//! musivum is a library first: it does not own the HTTP server. What it offers here are
//! functions over the `http` crate's request types (`http::request::Parts`, its headers,
//! URI and extensions), which any server calls, and a WebSocket that comes in through the
//! small trait [`SignalWebSocket`]. Upstream this layer is middleware and handlers of Go's
//! `net/http`; here a function answers what the middleware would have done (grants put
//! into the request's extensions, or a status with a text), and the server writes the
//! response.
//!
//! - [`ApiKeyAuthMiddleware`] (`auth.rs`): the access token of a request, verified; the
//!   permission checks on the grants it carries.
//! - [`validate_connect_request`], [`parse_client_info`], [`augment_client_info`] and the
//!   other helpers of `utils.rs`.
//! - [`WsSignalConnection`] (`wsprotocol.rs`): requests and responses over the signalling
//!   WebSocket, in protobuf or in its JSON form.
//! - [`LocalStore`], [`StandardRoomAllocator`] and the traits they implement
//!   (`interfaces.rs`): the rooms and participants of the node.
//!
//! [`useragent`] is written new: it stands in for the user agent parser the original uses.

mod auth;
mod errors;
mod interfaces;
mod localstore;
mod roomallocator;
pub mod useragent;
mod utils;
mod wsprotocol;

pub use auth::*;
pub use errors::*;
pub use interfaces::*;
pub use localstore::*;
pub use roomallocator::*;
pub use utils::*;
pub use wsprotocol::*;
