//! LiveKit's `pkg/rtc/signalling`: building and reading signal messages.
//!
//! Three interfaces that a participant's signalling is made of: [`ParticipantSignalling`]
//! builds every message a participant sends its client, [`ParticipantSignaller`] writes them
//! to the participant's signal connection, and [`ParticipantSignalHandler`] reads what the
//! client sends and calls the participant.
//!
//! The participant the handler and the signaller are given is a
//! [`types::LocalParticipant`](crate::rtc::types::LocalParticipant).

mod errors;
mod interfaces;
mod signalhandler;
mod signalhandlerunimplemented;
mod signallerasync;
mod signallerasyncbase;
mod signallerunimplemented;
// The file has the name of its package, as upstream.
#[allow(clippy::module_inception)]
mod signalling;
mod signallingunimplemented;

pub use errors::*;
pub use interfaces::*;
pub use signalhandler::*;
pub use signalhandlerunimplemented::*;
pub use signallerasync::*;
pub use signallerunimplemented::*;
pub use signalling::*;
pub use signallingunimplemented::*;
