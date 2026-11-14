// Ported from LiveKit, pkg/rtc/updatatrackmanager.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the fields `lock` guards are in the lock; the
// participant of the params is a `Weak` (see below), and a manager whose participant is
// gone tells no listener; `to_proto` returns the infos by value. The tests are new: the
// original has none.
// Who holds whom: the manager holds the data tracks strongly until they are removed or it
// is closed. It belongs to its participant, which it holds weakly.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::RwLock;

use crate::protocol::datatrack;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::rtc::types;

pub struct UpDataTrackManagerParams {
    pub logger: Logger,
    pub participant: Weak<dyn types::Participant>,
}

pub struct UpDataTrackManager {
    params: UpDataTrackManagerParams,

    // what `lock sync.RWMutex` guards upstream
    inner: RwLock<UpDataTrackManagerInner>,
}

#[derive(Default)]
struct UpDataTrackManagerInner {
    data_tracks: HashMap<u16, Arc<dyn types::DataTrack>>,
    closed: bool,
}

impl UpDataTrackManager {
    pub fn new(params: UpDataTrackManagerParams) -> Self {
        Self {
            params,
            inner: RwLock::new(UpDataTrackManagerInner::default()),
        }
    }

    pub fn close(&self) {
        let data_tracks = {
            let mut inner = self.inner.write();
            if inner.closed {
                return;
            }

            inner.closed = true;

            std::mem::take(&mut inner.data_tracks)
        };

        for t in data_tracks.values() {
            t.close();
        }
    }

    pub fn add_published_data_track(&self, dt: Arc<dyn types::DataTrack>) {
        self.inner
            .write()
            .data_tracks
            .insert(dt.pub_handle(), dt.clone());

        if let Some(participant) = self.params.participant.upgrade() {
            participant
                .get_participant_listener()
                .on_data_track_published(participant.clone(), dt);
        }
    }

    pub fn remove_published_data_track(&self, dt: Arc<dyn types::DataTrack>) {
        let mut found = false;
        let pub_handle = dt.pub_handle();
        {
            let mut inner = self.inner.write();
            if inner
                .data_tracks
                .get(&pub_handle)
                .is_some_and(|data_track| Arc::ptr_eq(data_track, &dt))
            {
                inner.data_tracks.remove(&pub_handle);
                found = true;
            }
        }

        if found {
            dt.close();

            if let Some(participant) = self.params.participant.upgrade() {
                participant
                    .get_participant_listener()
                    .on_data_track_unpublished(participant.clone(), dt);
            }
        }
    }

    pub fn get_published_data_tracks(&self) -> Vec<Arc<dyn types::DataTrack>> {
        let inner = self.inner.read();

        inner.data_tracks.values().cloned().collect()
    }

    pub fn get_published_data_track(&self, handle: u16) -> Option<Arc<dyn types::DataTrack>> {
        let inner = self.inner.read();

        inner.data_tracks.get(&handle).cloned()
    }

    pub fn handle_received_data_track_message(
        &self,
        data: &[u8],
        packet: &datatrack::Packet,
        arrival_time: i64,
    ) {
        let dt = self.inner.read().data_tracks.get(&packet.handle).cloned();
        let Some(dt) = dt else {
            return;
        };

        dt.handle_packet(data, packet, arrival_time);
    }

    pub fn to_proto(&self) -> Vec<livekit::DataTrackInfo> {
        let inner = self.inner.read();

        let mut data_track_infos = Vec::new();
        for dt in inner.data_tracks.values() {
            data_track_infos.push(dt.to_proto());
        }

        data_track_infos
    }
}

#[cfg(test)]
#[path = "updatatrackmanager_test.rs"]
mod updatatrackmanager_test;
