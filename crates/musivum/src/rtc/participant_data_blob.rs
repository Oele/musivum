// Ported from LiveKit, pkg/rtc/participant_data_blob.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the blobs, pointers to messages of the protocol that
// the original hands out again, are held and returned as `Arc`s; the map is keyed by the key
// message itself, which is hashable here, where the original keys it by the text form of the
// key (`String()`); a key that may be nil is an `Option`; `lock` guards the map, so the map
// is in the lock.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::protocol::livekit;
use crate::protocol::logger::Logger;

pub struct ParticipantDataBlobParams {
    pub logger: Logger,
}

pub struct ParticipantDataBlob {
    #[allow(dead_code)] // not read upstream either
    params: ParticipantDataBlobParams,
    // what `lock sync.Mutex` guards upstream
    blobs: Mutex<HashMap<livekit::DataBlobKey, Arc<livekit::DataBlob>>>,
}

impl ParticipantDataBlob {
    pub fn new(params: ParticipantDataBlobParams) -> Self {
        Self {
            params,
            blobs: Mutex::new(HashMap::new()),
        }
    }

    pub fn add(&self, db: Arc<livekit::DataBlob>) {
        let mut blobs = self.blobs.lock();

        let Some(key) = db.key.clone() else {
            return;
        };

        blobs.insert(key, db);
    }

    pub fn delete(&self, db_key: Option<&livekit::DataBlobKey>) {
        let mut blobs = self.blobs.lock();

        let Some(db_key) = db_key else {
            return;
        };

        blobs.remove(db_key);
    }

    pub fn get(&self, db_key: Option<&livekit::DataBlobKey>) -> Option<Arc<livekit::DataBlob>> {
        let blobs = self.blobs.lock();

        let db_key = db_key?;

        let db = blobs.get(db_key)?;

        Some(db.clone())
    }

    pub fn get_all(&self) -> Vec<Arc<livekit::DataBlob>> {
        let blobs = self.blobs.lock();

        let mut all = Vec::with_capacity(blobs.len());
        for db in blobs.values() {
            all.push(db.clone());
        }
        all
    }
}

// -------------------------------

#[cfg(test)]
#[path = "participant_data_blob_test.rs"]
mod participant_data_blob_test;
