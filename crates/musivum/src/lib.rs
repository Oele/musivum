//! musivum: a media server for calls that speaks LiveKit's protocol.
//!
//! A port to Rust of the media-forwarding part of LiveKit's server. The modules mirror the
//! paths of what they are ported from (`docs/port-map.md` in the repository):
//!
//! - [`sfu`], from LiveKit's `pkg/sfu`: receiving, buffering and forwarding media.
//! - [`rtc`], from LiveKit's `pkg/rtc`: rooms, participants, tracks and subscriptions.
//! - [`mediatransportutil`], from `livekit/mediatransportutil`.
//! - [`protocol`], from `livekit/protocol`.
//! - [`telemetry`], from LiveKit's `pkg/telemetry`: the interface the ported code reports
//!   what happens through (with nothing behind it yet), and the counters of its `prometheus`
//!   package, without Prometheus.
//! - [`webrtc`]: the packet types and small descriptions that LiveKit takes from pion.
//! - [`go`]: stand-ins for what the ported code uses of Go's standard library.

// The original uses fields of the protocol that are deprecated there, and so does the port.
#![allow(deprecated)]

pub mod clientconfiguration;
pub mod go;
pub mod mediatransportutil;
pub mod protocol;
pub mod routing;
pub mod rtc;
pub mod sfu;
pub mod telemetry;
pub mod utils;
pub mod webrtc;

#[cfg(test)]
pub mod testutils;
