// Ported from LiveKit, pkg/service/roomallocator.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, for one node: there is no router and no node selector
// (`NewRoomAllocator` takes neither, and cannot fail without the selector), and
// `SelectRoomNode` does what is left of it on one node (see there). Named room
// configurations are not taken (see `apply_named_room_configuration`). `CreateRoom` returns
// a copy of the request where the original returns the request itself.

use std::sync::Arc;

use super::{Error, ObjectStore, RoomAllocator, ensure_create_permission};
use crate::config::{Config, RoomConfig};
use crate::go::time;
use crate::protocol::auth::ClaimGrants;
use crate::protocol::livekit;
use crate::protocol::utils::{self, guid};

pub struct StandardRoomAllocator {
    config: Arc<Config>,
    room_store: Arc<dyn ObjectStore>,
}

pub fn new_room_allocator(conf: Arc<Config>, rs: Arc<dyn ObjectStore>) -> Arc<dyn RoomAllocator> {
    Arc::new(StandardRoomAllocator {
        config: conf,
        room_store: rs,
    })
}

// Unlocks the room when CreateRoom returns: the original's deferred UnlockRoom.
struct RoomUnlocker<'a> {
    room_store: &'a dyn ObjectStore,
    room_name: &'a livekit::RoomName,
    token: String,
}

impl Drop for RoomUnlocker<'_> {
    fn drop(&mut self) {
        let _ = self.room_store.unlock_room(self.room_name, &self.token);
    }
}

impl RoomAllocator for StandardRoomAllocator {
    fn auto_create_enabled(&self) -> bool {
        self.config.room.auto_create
    }

    // CreateRoom creates a new room from a request and allocates it to a node to handle
    // it'll also monitor its state, and cleans it up when appropriate
    fn create_room(
        &self,
        req: &livekit::CreateRoomRequest,
        _is_explicit: bool,
    ) -> Result<
        (
            livekit::Room,
            Option<livekit::RoomInternal>,
            livekit::CreateRoomRequest,
            bool,
        ),
        Error,
    > {
        let room_name = livekit::RoomName::from(req.name.as_str());
        let token = self.room_store.lock_room(&room_name, 5 * time::SECOND)?;
        let _unlock = RoomUnlocker {
            room_store: self.room_store.as_ref(),
            room_name: &room_name,
            token,
        };

        // find existing room and update it
        let mut created = false;
        let (mut rm, mut internal) = match self.room_store.load_room(&room_name, true) {
            Err(Error::RoomNotFound) => {
                created = true;
                let now = time::now();
                let mut rm = livekit::Room {
                    sid: guid::new(utils::ROOM_PREFIX),
                    name: req.name.clone(),
                    creation_time: now.unix_secs(),
                    creation_time_ms: now.unix_milli(),
                    turn_password: utils::random_secret(),
                    ..Default::default()
                };
                let mut internal = livekit::RoomInternal::default();
                apply_default_room_config(&mut rm, &mut internal, &self.config.room);
                (rm, Some(internal))
            }
            Err(err) => return Err(err),
            Ok(found) => found,
        };

        let req = self.apply_named_room_configuration(req)?;

        if req.empty_timeout > 0 {
            rm.empty_timeout = req.empty_timeout;
        }
        if req.departure_timeout > 0 {
            rm.departure_timeout = req.departure_timeout;
        }
        if req.max_participants > 0 {
            rm.max_participants = req.max_participants;
        }
        if !req.metadata.is_empty() {
            rm.metadata = req.metadata.clone();
        }
        // Where the original writes to a room's internal settings that were stored as nil
        // (which it would not survive), new ones are made here.
        if let Some(egress) = &req.egress {
            if let Some(participant) = &egress.participant {
                internal.get_or_insert_default().participant_egress = Some(participant.clone());
            }
            if let Some(tracks) = &egress.tracks {
                internal.get_or_insert_default().track_egress = Some(tracks.clone());
            }
        }
        // `req.Agents != nil`: a list that is there but empty is no list in protobuf
        if !req.agents.is_empty() {
            internal.get_or_insert_default().agent_dispatches = req.agents.clone();
        }
        if req.min_playout_delay > 0 || req.max_playout_delay > 0 {
            internal.get_or_insert_default().playout_delay = Some(livekit::PlayoutDelay {
                enabled: true,
                min: req.min_playout_delay,
                max: req.max_playout_delay,
            });
        }
        if req.sync_streams {
            internal.get_or_insert_default().sync_streams = true;
        }

        self.room_store.store_room(&rm, internal.as_ref())?;

        Ok((rm, internal, req, created))
    }

    fn select_room_node(
        &self,
        _room_name: &livekit::RoomName,
        _node_id: &livekit::NodeId,
    ) -> Result<(), Error> {
        // On one node the room is on this node: the local router's GetNodeForRoom answers
        // with it and its SetNodeForRoom does nothing. What the original checks besides is
        // whether that node is still available and whether it has reached the limits of the
        // configuration (`limit.num_tracks`, `limit.bytes_per_sec`, then
        // `routing.ErrNodeLimitReached`); both are read from the node's statistics, which
        // musivum does not collect (the local node and its statistics are not ported). So
        // there is nothing to refuse here; `validate_connect_request` checks the limits
        // against a node it is given.
        Ok(())
    }

    fn validate_create_room(
        &self,
        grants: Option<&ClaimGrants>,
        room_name: &livekit::RoomName,
    ) -> Result<(), Error> {
        // when auto create is disabled, we'll check to ensure it's already created
        if !self.config.room.auto_create && ensure_create_permission(grants).is_err() {
            self.room_store.load_room(room_name, false)?;
        }
        Ok(())
    }
}

pub(super) fn apply_default_room_config(
    room: &mut livekit::Room,
    internal: &mut livekit::RoomInternal,
    conf: &RoomConfig,
) {
    room.empty_timeout = conf.empty_timeout;
    room.departure_timeout = conf.departure_timeout;
    room.max_participants = conf.max_participants;
    for codec in &conf.enabled_codecs {
        room.enabled_codecs.push(livekit::Codec {
            mime: codec.mime.clone(),
            fmtp_line: codec.fmtp_line.clone(),
        });
    }
    internal.playout_delay = Some(livekit::PlayoutDelay {
        enabled: conf.playout_delay.enabled,
        min: conf.playout_delay.min as u32,
        max: conf.playout_delay.max as u32,
    });
    internal.sync_streams = conf.sync_streams;
}

impl StandardRoomAllocator {
    // Named room configurations (`room.room_configurations`) are left out: the configuration
    // accepts them and ignores them. So no preset is known here, and a request that names
    // one is refused, as the original refuses a preset its configuration does not have. The
    // original's copying of a configuration's settings into the request is not here.
    fn apply_named_room_configuration(
        &self,
        req: &livekit::CreateRoomRequest,
    ) -> Result<livekit::CreateRoomRequest, Error> {
        if req.room_preset.is_empty() {
            return Ok(req.clone());
        }

        Err(Error::UnknownRoomConfiguration)
    }
}

#[cfg(test)]
#[path = "roomallocator_test.rs"]
mod roomallocator_test;
