//! LiveKit's `pkg/rtc`: rooms, participants, tracks, subscriptions and the peer connections
//! they are held over. Ported so far are the small files that stand on nothing else of the
//! package: the errors, what a client's SDK and version imply, the helpers of `utils.go`,
//! the deduper of user packets, the shadow pacer of subscribes and the data blobs of a
//! participant.

pub mod dynacast;
pub mod signalling;
pub mod supervisor;
pub mod transport;
pub mod types;

mod clientinfo;
mod config;
mod datadowntrack;
mod datatrack;
mod datatrack_stats;
mod errors;
mod mediaengine;
mod medialossproxy;
mod mediatrack;
mod mediatrackreceiver;
mod mediatracksubscriptions;
mod participant_data_blob;
mod participant_data_blob_handler;
mod room;
mod roomtrackmanager;
mod signalanddatastats;
mod subscribedtrack;
mod subscribepacer;
mod subscriptionmanager;
#[cfg(test)]
pub mod testutils;
// transport.go: under another name, as `transport` is the subpackage `pkg/rtc/transport`
mod transport_impl;
mod transportmanager;
mod updatatrackmanager;
mod uptrackmanager;
mod user_packet_deduper;
mod utils;
mod wrappedreceiver;

pub use clientinfo::*;
pub use config::*;
pub use datadowntrack::*;
pub use datatrack::*;
pub use errors::*;
pub use mediaengine::*;
pub use medialossproxy::*;
pub use mediatrack::*;
pub use mediatrackreceiver::*;
pub use mediatracksubscriptions::*;
pub use participant_data_blob::*;
pub use participant_data_blob_handler::*;
pub use room::*;
pub use roomtrackmanager::*;
pub use signalanddatastats::*;
pub use subscribedtrack::*;
pub use subscribepacer::*;
pub use subscriptionmanager::*;
pub use transport_impl::*;
pub use transportmanager::*;
pub use updatatrackmanager::*;
pub use uptrackmanager::*;
pub use user_packet_deduper::*;
pub use utils::*;
pub use wrappedreceiver::*;
