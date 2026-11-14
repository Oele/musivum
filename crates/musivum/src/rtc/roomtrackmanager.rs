// Ported from LiveKit, pkg/rtc/roomtrackmanager.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. What `lock` guards is in an inner struct. A
// `TrackInfo` and a `DataTrackInfo` are kept and handed out by value (`get_track_info` gives
// a copy, `None` where the original gives nil), their track is never nil (so
// `get_track_info` does not check for it), and two tracks are the same one when they are the
// same allocation. The loops that take a track out of its list in place are `Vec::retain`.
// The notifiers are `Arc`s, as `utils::ChangeNotifierManager` hands them out. The tests are
// new: the original has none.
// Who holds whom: the manager holds the tracks that were added until they are removed, and
// its notifiers; a notifier holds its observers, which is whoever waits for the track.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::rtc::types;
use crate::utils;
use crate::{infow, warnw};

// RoomTrackManager holds tracks that are published to the room
pub struct RoomTrackManager {
    logger: Logger,

    lock: RwLock<RoomTrackManagerInner>,
    changed_notifier: utils::ChangeNotifierManager,
    removed_notifier: utils::ChangeNotifierManager,
}

// What `lock sync.RWMutex` guards upstream.
struct RoomTrackManagerInner {
    tracks: HashMap<livekit::TrackId, Vec<TrackInfo>>,
    data_tracks: HashMap<livekit::TrackId, Vec<DataTrackInfo>>,
}

#[derive(Clone)]
pub struct TrackInfo {
    pub track: Arc<dyn types::MediaTrack>,
    pub publisher_identity: livekit::ParticipantIdentity,
    pub publisher_id: livekit::ParticipantId,
}

#[derive(Clone)]
pub struct DataTrackInfo {
    pub data_track: Arc<dyn types::DataTrack>,
    pub publisher_identity: livekit::ParticipantIdentity,
    pub publisher_id: livekit::ParticipantId,
}

impl RoomTrackManager {
    pub fn new(logger: Logger) -> RoomTrackManager {
        RoomTrackManager {
            logger,
            lock: RwLock::new(RoomTrackManagerInner {
                tracks: HashMap::new(),
                data_tracks: HashMap::new(),
            }),
            changed_notifier: utils::ChangeNotifierManager::new(),
            removed_notifier: utils::ChangeNotifierManager::new(),
        }
    }

    pub fn add_track(
        &self,
        track: Arc<dyn types::MediaTrack>,
        publisher_identity: &livekit::ParticipantIdentity,
        publisher_id: &livekit::ParticipantId,
    ) {
        let track_id = track.id();
        let mut r = self.lock.write();
        if let Some(infos) = r.tracks.get(&track_id) {
            for info in infos {
                if Arc::ptr_eq(&info.track, &track) {
                    drop(r);
                    infow!(self.logger, "not adding duplicate track", "trackID" => track_id);
                    return;
                }
            }
        }
        r.tracks
            .entry(track_id.clone())
            .or_default()
            .push(TrackInfo {
                track,
                publisher_identity: publisher_identity.clone(),
                publisher_id: publisher_id.clone(),
            });
        drop(r);

        self.notify_track_changed(&track_id);
    }

    pub fn remove_track(&self, track: Arc<dyn types::MediaTrack>) {
        let track_id = track.id();
        let mut r = self.lock.write();
        // ensure we are removing the same track as added
        let Some(infos) = r.tracks.get_mut(&track_id) else {
            return;
        };

        let mut num_removed = 0;
        infos.retain(|info| {
            if Arc::ptr_eq(&info.track, &track) {
                num_removed += 1;
                false
            } else {
                true
            }
        });
        if infos.is_empty() {
            r.tracks.remove(&track_id);
        }
        drop(r);
        if num_removed == 0 {
            return;
        }
        if num_removed > 1 {
            warnw!(
                self.logger,
                "removed multiple tracks", nil,
                "trackID" => track_id,
                "numRemoved" => num_removed,
            );
        }

        let n = self.removed_notifier.get_notifier(track_id.as_str());
        if let Some(n) = n {
            n.notify_changed();
        }

        self.changed_notifier
            .remove_notifier(track_id.as_str(), true);
        self.removed_notifier
            .remove_notifier(track_id.as_str(), true);
    }

    pub fn get_track_info(&self, track_id: &livekit::TrackId) -> Option<TrackInfo> {
        let r = self.lock.read();

        // earliest added track is used till it is removed
        let info = r.tracks.get(track_id)?.first()?;

        // when track is about to close, do not resolve
        if !info.track.is_open() {
            return None;
        }
        Some(info.clone())
    }

    pub fn notify_track_changed(&self, track_id: &livekit::TrackId) {
        let n = self.changed_notifier.get_notifier(track_id.as_str());
        if let Some(n) = n {
            n.notify_changed();
        }
    }

    // HasObservers lets caller know if the current media track has any observers
    // this is used to signal interest in the track. when another MediaTrack with the same
    // trackID is being used, track is not considered to be observed.
    pub fn has_observers(&self, track: Arc<dyn types::MediaTrack>) -> bool {
        let n = self.changed_notifier.get_notifier(track.id().as_str());
        if !n.is_some_and(|n| n.has_observers()) {
            return false;
        }

        let info = self.get_track_info(&track.id());
        info.is_some_and(|info| Arc::ptr_eq(&info.track, &track))
    }

    pub fn get_or_create_track_change_notifier(
        &self,
        track_id: &livekit::TrackId,
    ) -> Arc<utils::ChangeNotifier> {
        self.changed_notifier
            .get_or_create_notifier(track_id.as_str())
    }

    pub fn get_or_create_track_remove_notifier(
        &self,
        track_id: &livekit::TrackId,
    ) -> Arc<utils::ChangeNotifier> {
        self.removed_notifier
            .get_or_create_notifier(track_id.as_str())
    }

    pub fn add_data_track(
        &self,
        data_track: Arc<dyn types::DataTrack>,
        publisher_identity: &livekit::ParticipantIdentity,
        publisher_id: &livekit::ParticipantId,
    ) {
        let track_id = data_track.id();
        let mut r = self.lock.write();
        if let Some(infos) = r.data_tracks.get(&track_id) {
            for info in infos {
                if Arc::ptr_eq(&info.data_track, &data_track) {
                    drop(r);
                    infow!(self.logger, "not adding duplicate data track", "trackID" => track_id);
                    return;
                }
            }
        }
        r.data_tracks
            .entry(track_id.clone())
            .or_default()
            .push(DataTrackInfo {
                data_track,
                publisher_identity: publisher_identity.clone(),
                publisher_id: publisher_id.clone(),
            });
        drop(r);

        self.notify_track_changed(&track_id);
    }

    pub fn remove_data_track(&self, data_track: Arc<dyn types::DataTrack>) {
        let track_id = data_track.id();
        let mut r = self.lock.write();
        // ensure we are removing the same track as added
        let Some(infos) = r.data_tracks.get_mut(&track_id) else {
            return;
        };

        let mut num_removed = 0;
        infos.retain(|info| {
            if Arc::ptr_eq(&info.data_track, &data_track) {
                num_removed += 1;
                false
            } else {
                true
            }
        });
        if infos.is_empty() {
            r.data_tracks.remove(&track_id);
        }
        drop(r);
        if num_removed == 0 {
            return;
        }
        if num_removed > 1 {
            warnw!(
                self.logger,
                "removed multiple data tracks", nil,
                "trackID" => track_id,
                "numRemoved" => num_removed,
            );
        }

        let n = self.removed_notifier.get_notifier(track_id.as_str());
        if let Some(n) = n {
            n.notify_changed();
        }

        self.changed_notifier
            .remove_notifier(track_id.as_str(), true);
        self.removed_notifier
            .remove_notifier(track_id.as_str(), true);
    }

    pub fn get_data_track_info(&self, track_id: &livekit::TrackId) -> Option<DataTrackInfo> {
        let r = self.lock.read();

        // earliest added data track is used till it is removed
        r.data_tracks.get(track_id)?.first().cloned()
    }

    pub fn report(&self) -> (usize, usize) {
        let r = self.lock.read();

        (r.tracks.len(), r.data_tracks.len())
    }
}

#[cfg(test)]
#[path = "roomtrackmanager_test.rs"]
mod roomtrackmanager_test;
