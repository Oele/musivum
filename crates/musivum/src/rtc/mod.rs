//! LiveKit's `pkg/rtc`: rooms, participants, tracks, subscriptions and the peer connections
//! they are held over. Ported so far are the small files that stand on nothing else of the
//! package: the errors, what a client's SDK and version imply, the helpers of `utils.go`,
//! the deduper of user packets, the shadow pacer of subscribes and the data blobs of a
//! participant.

pub mod dynacast;
pub mod signalling;
pub mod transport;
pub mod types;

mod clientinfo;
mod errors;
mod participant_data_blob;
mod subscribepacer;
mod subscriptionmanager;
#[cfg(test)]
pub mod testutils;
mod user_packet_deduper;
mod utils;

pub use clientinfo::*;
pub use errors::*;
pub use participant_data_blob::*;
pub use subscribepacer::*;
pub use subscriptionmanager::*;
pub use user_packet_deduper::*;
pub use utils::*;
