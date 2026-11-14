//! What musivum has of LiveKit's `pkg/telemetry`: the interface that the ported `pkg/rtc`
//! reports what happens through ([`TelemetryService`], with the types its methods name), an
//! implementation of it that does nothing ([`NullTelemetryService`]), and the counters the
//! ported code reports to, in [`prometheus`]. The service behind the interface, which
//! upstream turns the calls into analytics events, statistics and webhooks with, is not
//! ported; `docs/port-map.md` says what becomes of it.

mod analyticsservice;
pub mod prometheus;
mod stats;
mod statsworker;
mod telemetryservice;

pub use analyticsservice::*;
pub use stats::*;
pub use statsworker::*;
pub use telemetryservice::*;
