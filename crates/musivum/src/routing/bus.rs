//! What psrpc's local message bus carries between the router and the servers of one node.
//! Written new for musivum.
//!
//! Upstream even a single node reaches its own room manager and signal server through psrpc
//! over an in-process bus: the router's clients (`SignalClient`, `RoomManagerClient`) send to
//! the node's ID, and the servers, made later and with the router in hand, register under it
//! (`RegisterAllNodeTopics`). The bus is what lets the router be made before what it calls.
//! Here it is that and no more: a server registers under its node's ID, a client calls it
//! directly. A call to a node with no server registered gets [`Error::RequestTimedOut`] at
//! once, where psrpc gives the same error after its timeout (three seconds).
//!
//! The two traits are what psrpc generates for the services of `rpc` (`SignalServerImpl`,
//! `RoomManagerServerImpl`), as far as one node calls them:
//!
//! - [`SignalServerImpl::relay_signal`] is `RelaySignal` given what its stream carries: the
//!   `StartSession` of the first request, the source of the requests the client writes after
//!   it, and the sink of the responses to the client (see `signal.rs`).
//! - [`RoomManagerServerImpl::create_room`] is `CreateRoom`.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use super::{Error, MessageSink, MessageSource};
use crate::protocol::livekit;

/// psrpc's `rpc.SignalServerImpl`, which `pkg/service`'s signal server implements.
pub trait SignalServerImpl: Send + Sync {
    /// `RelaySignal` once the first request has come: what the server half of the original
    /// does after reading `req.StartSession` from the stream. The call returns when the
    /// server has handed the session on; its error is the server's, which upstream closes
    /// the stream with (here the stream is closed by the caller, see `signal.rs`).
    fn relay_signal(
        &self,
        start_session: livekit::StartSession,
        request_source: Arc<dyn MessageSource>,
        response_sink: Arc<dyn MessageSink>,
    ) -> Result<(), Error>;
}

/// psrpc's `rpc.RoomManagerServerImpl`, which `pkg/service`'s room manager implements.
pub trait RoomManagerServerImpl: Send + Sync {
    fn create_room(&self, req: &livekit::CreateRoomRequest) -> Result<livekit::Room, Error>;
}

/// The bus of one process: Go's `psrpc.MessageBus` as `psrpc.NewLocalMessageBus()` makes it.
#[derive(Default)]
pub struct MessageBus {
    signal_servers: RwLock<HashMap<livekit::NodeId, Arc<dyn SignalServerImpl>>>,
    room_manager_servers: RwLock<HashMap<livekit::NodeId, Arc<dyn RoomManagerServerImpl>>>,
}

/// Go's `psrpc.NewLocalMessageBus()`.
pub fn new_local_message_bus() -> Arc<MessageBus> {
    Arc::new(MessageBus::default())
}

impl MessageBus {
    /// The signal server's `RegisterAllNodeTopics(nodeID)`.
    pub fn register_signal_server(
        &self,
        node_id: &livekit::NodeId,
        server: Arc<dyn SignalServerImpl>,
    ) {
        self.signal_servers.write().insert(node_id.clone(), server);
    }

    /// The signal server's `Kill()`.
    pub fn deregister_signal_server(&self, node_id: &livekit::NodeId) {
        self.signal_servers.write().remove(node_id);
    }

    /// The room manager server's `RegisterAllNodeTopics(nodeID)`.
    pub fn register_room_manager_server(
        &self,
        node_id: &livekit::NodeId,
        server: Arc<dyn RoomManagerServerImpl>,
    ) {
        self.room_manager_servers
            .write()
            .insert(node_id.clone(), server);
    }

    /// The room manager server's `Kill()`.
    pub fn deregister_room_manager_server(&self, node_id: &livekit::NodeId) {
        self.room_manager_servers.write().remove(node_id);
    }

    pub(super) fn signal_server(
        &self,
        node_id: &livekit::NodeId,
    ) -> Result<Arc<dyn SignalServerImpl>, Error> {
        self.signal_servers
            .read()
            .get(node_id)
            .cloned()
            .ok_or(Error::RequestTimedOut)
    }

    pub(super) fn room_manager_server(
        &self,
        node_id: &livekit::NodeId,
    ) -> Result<Arc<dyn RoomManagerServerImpl>, Error> {
        self.room_manager_servers
            .read()
            .get(node_id)
            .cloned()
            .ok_or(Error::RequestTimedOut)
    }
}
