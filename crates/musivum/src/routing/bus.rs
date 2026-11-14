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
//!
//! The servers of the room service's calls into a room and a participant (`rpc.rs`:
//! [`RoomServerImpl`], [`ParticipantServerImpl`]) are registered under the room's and the
//! participant's topic, as the room manager registers them upstream when a room is made and
//! a participant joins (`NewTypedRoomServer(r, bus)` and `RegisterAllRoomTopics(topic)`).
//! Each registration has an ID, the psrpc server's own, by which it is taken away again
//! (the server's `Kill()`): a later registration under the same topic replaces an earlier
//! one, and taking the earlier one away then leaves the later in place, as killing the
//! replaced psrpc server leaves its successor subscribed. A call for a topic with no server
//! gets psrpc's [`PsrpcError::no_response`] at once.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use super::{
    Error, MessageSink, MessageSource, ParticipantServerImpl, ParticipantTopic, PsrpcError,
    RoomServerImpl, RoomTopic,
};
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

/// The ID of a server registered under a topic: psrpc's server ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ServerId(u64);

type RoomServers = HashMap<RoomTopic, (ServerId, Arc<dyn RoomServerImpl>)>;
type ParticipantServers = HashMap<ParticipantTopic, (ServerId, Arc<dyn ParticipantServerImpl>)>;

/// The bus of one process: Go's `psrpc.MessageBus` as `psrpc.NewLocalMessageBus()` makes it.
#[derive(Default)]
pub struct MessageBus {
    signal_servers: RwLock<HashMap<livekit::NodeId, Arc<dyn SignalServerImpl>>>,
    room_manager_servers: RwLock<HashMap<livekit::NodeId, Arc<dyn RoomManagerServerImpl>>>,
    room_servers: RwLock<RoomServers>,
    participant_servers: RwLock<ParticipantServers>,
    last_server_id: AtomicU64,
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

    /// `NewTypedRoomServer(svc, bus)` and its `RegisterAllRoomTopics(room)`: the calls to the
    /// room's topic reach `server` from now on.
    pub fn register_room_server(
        &self,
        room: &RoomTopic,
        server: Arc<dyn RoomServerImpl>,
    ) -> ServerId {
        let id = self.new_server_id();
        self.room_servers.write().insert(room.clone(), (id, server));
        id
    }

    /// The `Kill()` of the room server that `register_room_server` gave `id` for.
    pub fn deregister_room_server(&self, room: &RoomTopic, id: ServerId) {
        let mut servers = self.room_servers.write();
        if servers.get(room).is_some_and(|(current, _)| *current == id) {
            servers.remove(room);
        }
    }

    /// `NewTypedParticipantServer(svc, bus)` and its `RegisterAllParticipantTopics(participant)`.
    pub fn register_participant_server(
        &self,
        participant: &ParticipantTopic,
        server: Arc<dyn ParticipantServerImpl>,
    ) -> ServerId {
        let id = self.new_server_id();
        self.participant_servers
            .write()
            .insert(participant.clone(), (id, server));
        id
    }

    /// The `Kill()` of the participant server that `register_participant_server` gave `id` for.
    pub fn deregister_participant_server(&self, participant: &ParticipantTopic, id: ServerId) {
        let mut servers = self.participant_servers.write();
        if servers
            .get(participant)
            .is_some_and(|(current, _)| *current == id)
        {
            servers.remove(participant);
        }
    }

    fn new_server_id(&self) -> ServerId {
        ServerId(self.last_server_id.fetch_add(1, Ordering::Relaxed) + 1)
    }

    pub(super) fn room_server(
        &self,
        room: &RoomTopic,
    ) -> Result<Arc<dyn RoomServerImpl>, PsrpcError> {
        self.room_servers
            .read()
            .get(room)
            .map(|(_, server)| server.clone())
            .ok_or_else(PsrpcError::no_response)
    }

    pub(super) fn participant_server(
        &self,
        participant: &ParticipantTopic,
    ) -> Result<Arc<dyn ParticipantServerImpl>, PsrpcError> {
        self.participant_servers
            .read()
            .get(participant)
            .map(|(_, server)| server.clone())
            .ok_or_else(PsrpcError::no_response)
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
