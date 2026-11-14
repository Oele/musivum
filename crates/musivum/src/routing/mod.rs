//! LiveKit's `pkg/routing`, as far as one node needs it: what a participant's signalling is
//! written to and read from.
//!
//! musivum is one node. What LiveKit routes between nodes (the routers, the nodes and their
//! statistics, the relay of signalling over psrpc) is left out; what is here is the pair of
//! interfaces a participant's signalling goes through ([`MessageSink`], [`MessageSource`]),
//! the channel in the process that implements both ([`MessageChannel`]) and what a session is
//! started with ([`ParticipantInit`]). The endpoint a client is connected to writes the
//! client's requests into one channel, which the participant reads, and the participant
//! writes its responses into another, which the endpoint reads.
//!
//! [`Message`] and [`ReadChan`] are written new: what Go's `proto.Message` and
//! `<-chan proto.Message` are here.

mod errors;
mod interfaces;
mod message;
mod messagechannel;

pub use errors::*;
pub use interfaces::*;
pub use message::*;
pub use messagechannel::*;
