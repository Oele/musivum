// Ported from LiveKit, pkg/routing/roommanager.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, for one node and without psrpc. `RoomManagerClient` is
// `rpc.TypedRoomManagerClient`'s two methods (`CreateRoom` without the context and the
// request options, and `Close`). The client calls the room manager registered on the bus
// for the node (`bus.rs`) directly, on the caller's thread; a node with none gives
// `RequestTimedOut` at once. The retries of psrpc's interceptor (`CreateRoomAttempts` tries
// of `CreateRoomTimeout` each) are for requests that go over the bus and are not made: the
// configuration is kept, and not read. `NewRoomManagerClient` takes the bus where the
// original takes `rpc.ClientParams`, of which only the bus is left.

use std::sync::Arc;

use super::Error;
use super::bus::MessageBus;
use crate::config::RoomConfig;
use crate::protocol::livekit;

#[cfg_attr(test, musivum_fakes::fake)]
pub trait RoomManagerClient: Send + Sync {
    fn create_room(
        &self,
        node_id: &livekit::NodeId,
        req: &livekit::CreateRoomRequest,
    ) -> Result<livekit::Room, Error>;
    fn close(&self);
}

struct RoomManagerClientImpl {
    // `CreateRoomAttempts` and `CreateRoomTimeout`, for the retries of psrpc; not read here
    #[allow(dead_code)]
    config: RoomConfig,
    client: Arc<MessageBus>,
}

pub fn new_room_manager_client(
    bus: Arc<MessageBus>,
    config: RoomConfig,
) -> Result<Arc<dyn RoomManagerClient>, Error> {
    Ok(Arc::new(RoomManagerClientImpl {
        config,
        client: bus,
    }))
}

impl RoomManagerClient for RoomManagerClientImpl {
    fn create_room(
        &self,
        node_id: &livekit::NodeId,
        req: &livekit::CreateRoomRequest,
    ) -> Result<livekit::Room, Error> {
        self.client.room_manager_server(node_id)?.create_room(req)
    }

    fn close(&self) {}
}
