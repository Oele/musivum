//! LiveKit's `pkg/routing`, as far as one node needs it: the node, its statistics and limits,
//! and the start of a participant's session on it.
//!
//! musivum is one node. What LiveKit routes between nodes (`RedisRouter`, the relay of
//! signalling over psrpc to another node, the region aware choice of a node) is left out.
//! What is here:
//!
//! - the pair of interfaces a participant's signalling goes through ([`MessageSink`],
//!   [`MessageSource`]), the channel in the process that implements both
//!   ([`MessageChannel`]) and what a session is started with ([`ParticipantInit`]);
//! - the node ([`LocalNode`], [`LocalNodeImpl`]) and its statistics ([`NodeStats`]), which
//!   `telemetry::prometheus::get_node_stats` gathers with the protocol's `hwstats`, and the
//!   limits and choice of a node ([`selector`]);
//! - the routers ([`Router`], [`MessageRouter`], [`LocalRouter`]) and what they call on the
//!   node: the signal client ([`SignalClient`]), which starts a session on the node's signal
//!   server, and the room manager client ([`RoomManagerClient`]), both through the bus of
//!   the process ([`MessageBus`]) where upstream has psrpc's.
//!
//! The endpoint a client is connected to writes the client's requests into one channel,
//! which the participant reads, and the participant writes its responses into another,
//! which the endpoint reads.
//!
//! [`Message`] and [`ReadChan`] are written new: what Go's `proto.Message` and
//! `<-chan proto.Message` are here. So is `bus.rs` ([`MessageBus`]).

mod bus;
mod errors;
mod interfaces;
mod localrouter;
mod message;
mod messagechannel;
mod node;
mod nodestats;
mod roommanager;
mod signal;

pub mod selector;

pub use bus::*;
pub use errors::*;
pub use interfaces::*;
pub use localrouter::*;
pub use message::*;
pub use messagechannel::*;
pub use node::*;
pub use nodestats::*;
pub use roommanager::*;
pub use signal::*;
