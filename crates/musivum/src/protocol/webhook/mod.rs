//! The Go package `github.com/livekit/protocol/webhook`: webhooks, the signed HTTP POSTs that
//! tell a server's owner what happens in its rooms.
//!
//! A webhook is the protocol's JSON form of a `livekit::WebhookEvent`, sent with the content
//! type [`CONTENT_TYPE`] and an `Authorization` header that is an access token of the API
//! key: its `sha256` claim is the base64 of the body's SHA-256. [`receive`] and
//! [`receive_webhook_event`] check one on the receiving side.
//!
//! The notifiers send them: [`ResourceUrlNotifier`] queues the events of each resource (a
//! room, a participant, an egress, ...) on their own and drops what waits too long or too
//! deep, [`UrlNotifier`] queues them on a pool of workers and drops what overflows it, and
//! [`new_default_notifier`] makes a resource notifier for each URL of a [`WebHookConfig`],
//! with one more for the extra webhooks of an egress. Each retries a failed POST
//! ([`retryablehttp`], written new on reqwest). A notifier lives until it is stopped
//! ([`QueuedNotifier::stop`]): stopping it without force sends what is queued first.
//!
//! Two helpers that upstream takes from elsewhere are here because nothing else uses them yet:
//! `timeoutqueue` is `utils/timeoutqueue.go` of the protocol and `pool` is the `QueuePool`
//! of `github.com/frostbyte73/core`.

mod consts;
mod filter;
mod notifier;
mod pool;
mod resource_queue;
mod resource_url_notifier;
pub mod retryablehttp;
mod stats;
mod timeoutqueue;
mod url_notifier;
mod verifier;

pub use consts::*;
pub use notifier::*;
pub use resource_url_notifier::*;
pub use stats::*;
pub use timeoutqueue::*;
pub use url_notifier::*;
pub use verifier::*;

#[cfg(test)]
#[path = "webhook_test.rs"]
pub(crate) mod webhook_test;
