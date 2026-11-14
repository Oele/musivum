//! LiveKit's `pkg/telemetry`: the interface that the ported `pkg/rtc` reports what happens
//! through ([`TelemetryService`], with the types its methods name), the service behind it,
//! which turns the calls into webhooks, statistics and analytics events
//! ([`new_telemetry_service`]), an implementation that does nothing
//! ([`NullTelemetryService`]), and the counters the ported code reports to, in
//! [`prometheus`].
//!
//! The service sends webhooks through the notifier it is given (see
//! `crate::protocol::webhook`), counts into [`prometheus`], keeps a stats worker per
//! participant whose statistics it flushes every 30 seconds, and hands analytics events and
//! statistics to an [`AnalyticsService`]. LiveKit's own sends them to its analytics recorder
//! and is not ported: musivum's embedder gives [`NullAnalyticService`].
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use musivum::protocol::auth::SimpleKeyProvider;
//! use musivum::protocol::webhook::{self, WebHookConfig};
//! use musivum::telemetry::{self, NullAnalyticService};
//!
//! # async fn f() {
//! let config = WebHookConfig {
//!     urls: vec!["https://example.org/livekit-webhook".to_string()],
//!     api_key: "the-api-key".to_string(),
//!     ..webhook::DEFAULT_WEB_HOOK_CONFIG
//! };
//! let keys = Arc::new(SimpleKeyProvider::new("the-api-key", "the-api-secret"));
//! let notifier = webhook::new_default_notifier(&config, keys).unwrap();
//! let telemetry =
//!     telemetry::new_telemetry_service(Some(notifier.clone()), Arc::new(NullAnalyticService {}));
//! // ... the rooms report to `telemetry` ...
//! drop(telemetry);
//! // send what is queued, then stop
//! notifier.stop(false);
//! # }
//! ```

mod analyticsservice;
mod events;
pub mod prometheus;
mod stats;
mod statsconn;
mod statsworker;
mod telemetryservice;

pub use analyticsservice::*;
pub use stats::*;
pub use statsconn::*;
pub use statsworker::*;
pub use telemetryservice::*;
