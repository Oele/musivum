//! LiveKit's `pkg/clientconfiguration`: settings per kind of client. When a client joins, its
//! SDK, version, browser, operating system and device are looked up in a table of rules
//! ([`STATIC_CONFIGURATIONS`]) and it is sent the settings of the rules that apply, for
//! instance not to publish a codec that its browser gets wrong.
//!
//! The original writes the condition of a rule as an expression of a script language (tengo),
//! compiled when the server starts. musivum has no script interpreter: here the same
//! expression is Rust code, a closure over the client's fields
//! ([`script_match!`](crate::script_match)), and the types of those fields compare the way the
//! original's do. `semver.rs`, which is written new, says of two versions what
//! `golang.org/x/mod/semver` says.

mod conf;
mod r#match;
mod semver;
mod staticconfiguration;
mod types;

pub use conf::*;
pub use r#match::*;
pub use staticconfiguration::*;
pub use types::*;

/// The errors of the package. The original has no `ErrX` values: this is the error that
/// `ScriptMatch.Match` makes in place, with the same message. The port's own `ScriptMatch`
/// never gives it (an expression that does not give a boolean does not compile here); it is
/// what another implementation of [`Match`] has to say that it failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    // match.go
    #[error("invalid match expression result")]
    InvalidMatchExpressionResult,
}

#[cfg(test)]
#[path = "reference_test.rs"]
mod reference_test;
