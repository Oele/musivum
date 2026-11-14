//! The Go package `github.com/livekit/livekit-server/pkg/config`: LiveKit's configuration,
//! which is the struct a configuration file is read into ([`Config`]), its defaults
//! ([`DEFAULT_CONFIG`]) and what is checked and derived when it is loaded ([`Config::new`]).
//! A file written for LiveKit's server loads as it is. The parts of it that are for what
//! musivum leaves out are taken and not read ([`Config::ignored_sections`]).
//!
//! [`yaml`] is the reading of YAML into these structs, done the way the original's YAML
//! library does it. [`logger`], [`webhook`] and [`metric`] have the configuration structs
//! of packages that are not ported yet.

#[allow(clippy::module_inception)]
mod config;
mod fields;
pub mod logger;
pub mod metric;
pub mod webhook;
pub mod yaml;

pub use config::*;
