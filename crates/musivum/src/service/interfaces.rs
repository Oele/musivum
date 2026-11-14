// Ported from LiveKit, pkg/service/interfaces.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; only what one node needs: the stores of rooms and
// participants and the room allocator. Not here: `EgressStore`, `IngressStore`, `SIPStore`
// and `AgentStore`. The `context.Context` parameters are dropped; `ValidateCreateRoom`, which
// reads the request's grants from its context, takes them as a parameter. Messages are
// taken by reference and returned as values: a store hands out copies of what it keeps,
// where the original hands out its own pointers (`CreateRoom` changes a room it loaded and
// stores it again, which comes to the same). A room's internal settings that may be nil are
// an `Option`, and so are the room names `ListRooms` filters by (nil: all rooms).
// `OSSServiceStore` has a fake too, which counterfeiter does not make: it is a supertrait of
// `ObjectStore`, whose fake implements it.

use super::Error;
use crate::go::time::Duration;
use crate::protocol::auth::ClaimGrants;
use crate::protocol::livekit;

// encapsulates CRUD operations for room settings
#[cfg_attr(test, musivum_fakes::fake)]
pub trait ObjectStore: ServiceStore + OssServiceStore {
    // enable locking on a specific room to prevent race
    // returns a (lock uuid, error)
    fn lock_room(&self, room_name: &livekit::RoomName, duration: Duration)
    -> Result<String, Error>;
    fn unlock_room(&self, room_name: &livekit::RoomName, uid: &str) -> Result<(), Error>;

    fn store_room(
        &self,
        room: &livekit::Room,
        internal: Option<&livekit::RoomInternal>,
    ) -> Result<(), Error>;

    fn store_participant(
        &self,
        room_name: &livekit::RoomName,
        participant: &livekit::ParticipantInfo,
    ) -> Result<(), Error>;
    fn delete_participant(
        &self,
        room_name: &livekit::RoomName,
        identity: &livekit::ParticipantIdentity,
    ) -> Result<(), Error>;
}

#[cfg_attr(test, musivum_fakes::fake)]
pub trait ServiceStore: Send + Sync {
    fn load_room(
        &self,
        room_name: &livekit::RoomName,
        include_internal: bool,
    ) -> Result<(livekit::Room, Option<livekit::RoomInternal>), Error>;
    fn room_exists(&self, room_name: &livekit::RoomName) -> Result<bool, Error>;

    // ListRooms returns currently active rooms. if names is not nil, it'll filter and return
    // only rooms that match
    fn list_rooms(
        &self,
        room_names: Option<&[livekit::RoomName]>,
    ) -> Result<Vec<livekit::Room>, Error>;
}

#[cfg_attr(test, musivum_fakes::fake)]
pub trait OssServiceStore: Send + Sync {
    fn delete_room(&self, room_name: &livekit::RoomName) -> Result<(), Error>;
    fn has_participant(
        &self,
        room_name: &livekit::RoomName,
        identity: &livekit::ParticipantIdentity,
    ) -> Result<bool, Error>;
    fn load_participant(
        &self,
        room_name: &livekit::RoomName,
        identity: &livekit::ParticipantIdentity,
    ) -> Result<livekit::ParticipantInfo, Error>;
    fn list_participants(
        &self,
        room_name: &livekit::RoomName,
    ) -> Result<Vec<livekit::ParticipantInfo>, Error>;
}

// EgressStore and IngressStore: not ported (egress and ingress are left out).

#[cfg_attr(test, musivum_fakes::fake)]
pub trait RoomAllocator: Send + Sync {
    fn auto_create_enabled(&self) -> bool;
    fn select_room_node(
        &self,
        room_name: &livekit::RoomName,
        node_id: &livekit::NodeId,
    ) -> Result<(), Error>;
    // CreateRoom also returns the request with its room preset applied,
    // or req unchanged when the implementation does not resolve presets
    fn create_room(
        &self,
        req: &livekit::CreateRoomRequest,
        is_explicit: bool,
    ) -> Result<
        (
            livekit::Room,
            Option<livekit::RoomInternal>,
            livekit::CreateRoomRequest,
            bool,
        ),
        Error,
    >;
    fn validate_create_room(
        &self,
        grants: Option<&ClaimGrants>,
        room_name: &livekit::RoomName,
    ) -> Result<(), Error>;
}

// SIPStore and AgentStore: not ported (SIP and agents are left out).
