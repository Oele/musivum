// Ported from LiveKit, pkg/service/localstore.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the agent dispatches and jobs (`StoreAgentDispatch`,
// `DeleteAgentDispatch`, `ListAgentDispatches`, `StoreAgentJob`, `DeleteAgentJob` and their
// maps) are not here, since agents are left out. The store keeps copies of what it is given
// and hands out copies, where the original keeps and hands out the pointers it is given;
// `StoreRoom` therefore sets the creation time of a room without one on its copy, not on
// the caller's room. The global lock, a Go mutex locked by `LockRoom` and unlocked by
// `UnlockRoom`, is a flag with a condition variable, since a Rust mutex's guard cannot be
// held from one call to the next. The methods are in three blocks, one per trait they belong
// to, each in the original's order.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::{Condvar, Mutex, RwLock};

use super::{Error, ObjectStore, OssServiceStore, ServiceStore};
use crate::go::time::{self, Duration};
use crate::protocol::livekit;

// encapsulates CRUD operations for room settings
pub struct LocalStore {
    lock: RwLock<LocalStoreInner>,
    // `globalLock sync.Mutex`: whether it is locked, and where a locker waits for it
    global_lock: Mutex<bool>,
    global_unlocked: Condvar,
}

#[derive(Default)]
struct LocalStoreInner {
    // map of roomName => room
    rooms: HashMap<livekit::RoomName, livekit::Room>,
    room_internal: HashMap<livekit::RoomName, livekit::RoomInternal>,
    // map of roomName => { identity: participant }
    participants:
        HashMap<livekit::RoomName, HashMap<livekit::ParticipantIdentity, livekit::ParticipantInfo>>,
}

impl LocalStore {
    pub fn new() -> Arc<LocalStore> {
        Arc::new(LocalStore {
            lock: RwLock::new(LocalStoreInner::default()),
            global_lock: Mutex::new(false),
            global_unlocked: Condvar::new(),
        })
    }
}

impl ObjectStore for LocalStore {
    fn store_room(
        &self,
        room: &livekit::Room,
        internal: Option<&livekit::RoomInternal>,
    ) -> Result<(), Error> {
        let mut room = room.clone();
        if room.creation_time == 0 {
            let now = time::now();
            room.creation_time = now.unix_secs();
            room.creation_time_ms = now.unix_milli();
        }
        let room_name = livekit::RoomName::from(room.name.as_str());

        let mut inner = self.lock.write();
        inner.rooms.insert(room_name.clone(), room);
        match internal {
            Some(internal) => {
                inner.room_internal.insert(room_name, internal.clone());
            }
            None => {
                inner.room_internal.remove(&room_name);
            }
        }
        drop(inner);

        Ok(())
    }

    fn lock_room(
        &self,
        _room_name: &livekit::RoomName,
        _duration: Duration,
    ) -> Result<String, Error> {
        // local rooms lock & unlock globally
        let mut locked = self.global_lock.lock();
        while *locked {
            self.global_unlocked.wait(&mut locked);
        }
        *locked = true;
        Ok(String::new())
    }

    fn unlock_room(&self, _room_name: &livekit::RoomName, _uid: &str) -> Result<(), Error> {
        let mut locked = self.global_lock.lock();
        if !*locked {
            // what Go's sync.Mutex does on an unlock without a lock
            panic!("sync: unlock of unlocked mutex");
        }
        *locked = false;
        self.global_unlocked.notify_one();
        Ok(())
    }

    fn store_participant(
        &self,
        room_name: &livekit::RoomName,
        participant: &livekit::ParticipantInfo,
    ) -> Result<(), Error> {
        let mut inner = self.lock.write();
        let room_participants = inner.participants.entry(room_name.clone()).or_default();
        room_participants.insert(
            livekit::ParticipantIdentity::from(participant.identity.as_str()),
            participant.clone(),
        );
        Ok(())
    }

    fn delete_participant(
        &self,
        room_name: &livekit::RoomName,
        identity: &livekit::ParticipantIdentity,
    ) -> Result<(), Error> {
        let mut inner = self.lock.write();

        if let Some(room_participants) = inner.participants.get_mut(room_name) {
            room_participants.remove(identity);
        }
        Ok(())
    }
}

impl ServiceStore for LocalStore {
    fn load_room(
        &self,
        room_name: &livekit::RoomName,
        include_internal: bool,
    ) -> Result<(livekit::Room, Option<livekit::RoomInternal>), Error> {
        let inner = self.lock.read();

        let Some(room) = inner.rooms.get(room_name) else {
            return Err(Error::RoomNotFound);
        };

        let mut internal = None;
        if include_internal {
            internal = inner.room_internal.get(room_name).cloned();
        }

        Ok((room.clone(), internal))
    }

    fn room_exists(&self, room_name: &livekit::RoomName) -> Result<bool, Error> {
        match self.load_room(room_name, false) {
            Err(Error::RoomNotFound) => Ok(false),
            Err(err) => Err(err),
            Ok(_) => Ok(true),
        }
    }

    fn list_rooms(
        &self,
        room_names: Option<&[livekit::RoomName]>,
    ) -> Result<Vec<livekit::Room>, Error> {
        let inner = self.lock.read();
        let mut rooms = Vec::with_capacity(inner.rooms.len());
        for r in inner.rooms.values() {
            if room_names.is_none_or(|names| names.iter().any(|n| n.as_str() == r.name)) {
                rooms.push(r.clone());
            }
        }
        Ok(rooms)
    }
}

impl OssServiceStore for LocalStore {
    fn delete_room(&self, room_name: &livekit::RoomName) -> Result<(), Error> {
        let room = match self.load_room(room_name, false) {
            Err(Error::RoomNotFound) => return Ok(()),
            Err(err) => return Err(err),
            Ok((room, _)) => room,
        };

        let mut inner = self.lock.write();

        let name = livekit::RoomName::from(room.name.as_str());
        inner.participants.remove(&name);
        inner.rooms.remove(&name);
        inner.room_internal.remove(&name);
        // agentDispatches and agentJobs: not here
        Ok(())
    }

    fn load_participant(
        &self,
        room_name: &livekit::RoomName,
        identity: &livekit::ParticipantIdentity,
    ) -> Result<livekit::ParticipantInfo, Error> {
        let inner = self.lock.read();

        let Some(room_participants) = inner.participants.get(room_name) else {
            return Err(Error::ParticipantNotFound);
        };
        let Some(participant) = room_participants.get(identity) else {
            return Err(Error::ParticipantNotFound);
        };
        Ok(participant.clone())
    }

    fn has_participant(
        &self,
        room_name: &livekit::RoomName,
        identity: &livekit::ParticipantIdentity,
    ) -> Result<bool, Error> {
        // p != nil, utils.ScreenError(err, ErrParticipantNotFound)
        match self.load_participant(room_name, identity) {
            Ok(_) => Ok(true),
            Err(Error::ParticipantNotFound) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn list_participants(
        &self,
        room_name: &livekit::RoomName,
    ) -> Result<Vec<livekit::ParticipantInfo>, Error> {
        let inner = self.lock.read();

        let Some(room_participants) = inner.participants.get(room_name) else {
            // empty array
            return Ok(Vec::new());
        };

        let mut items = Vec::with_capacity(room_participants.len());
        for p in room_participants.values() {
            items.push(p.clone());
        }

        Ok(items)
    }
}

// StoreAgentDispatch, DeleteAgentDispatch, ListAgentDispatches, StoreAgentJob and
// DeleteAgentJob: not ported (agents are left out).
