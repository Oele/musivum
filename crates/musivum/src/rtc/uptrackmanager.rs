// Ported from LiveKit, pkg/rtc/uptrackmanager.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. `NewUpTrackManager` is `UpTrackManager::new` and gives
// an `Arc`, which keeps a `Weak` of itself for the callback it hands a published track. What
// `lock` guards is in an inner struct, the version of the permissions too (an atomic
// upstream that is only touched with the lock held); the functions named `...Locked` are
// methods of that struct, except `parse_subscription_permissions_locked`, which logs and so
// stays with the manager and is handed the guard. Its resolver gets the guard as well: it
// is how the lock is let go of around the call to `resolver_by_sid`, as upstream.
// `on_track_updated`, a plain field upstream that is written and read without a lock, has a
// lock of its own and is called with none held. The subscription permission is kept in an
// `Arc`, which `subscription_permission` hands out; the permissions per subscriber are
// copies of its entries where upstream keeps pointers into it, and the map that is nil
// upstream when everybody is allowed is `None`, as is the list of
// `get_allowed_subscribers`. `debug_info` gives the JSON object of `sfu::DebugInfo`; its
// branch for a `*MediaTrack`, which asks the track for its own debug info, is not here
// yet: `mediatrack.go` is not ported, so every track is described by its ID, kind and mute
// state. `ErrSubscriptionPermissionNeedsId` is `Error::SubscriptionPermissionNeedsId` in
// `errors.rs`.
// Who holds whom: the manager holds its published tracks. The callback it gives a track
// (`add_on_close`) holds the manager and the track weakly, since the track keeps the
// callback. The manager keeps the callbacks of `on_up_track_manager_close` and
// `on_published_track_updated`: the participant that owns the manager hands in closures
// that hold it weakly.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::{RwLock, RwLockWriteGuard};

use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::protocol::utils::{TimedVersion, TimedVersionGenerator};
use crate::rtc::Error;
use crate::rtc::types;
use crate::sfu;
use crate::{debugw, errorw, warnw};

pub struct UpTrackManagerParams {
    pub logger: Logger,
    pub version_generator: Arc<dyn TimedVersionGenerator>,
}

// UpTrackManager manages all uptracks from a participant
pub struct UpTrackManager {
    params: UpTrackManagerParams,

    lock: RwLock<UpTrackManagerInner>,

    // callbacks & handlers
    #[allow(clippy::type_complexity)]
    on_track_updated: RwLock<Option<Arc<dyn Fn(Arc<dyn types::MediaTrack>) + Send + Sync>>>,

    this: Weak<UpTrackManager>,
}

// What `lock sync.RWMutex` guards upstream.
struct UpTrackManagerInner {
    subscription_permission_version: TimedVersion,

    closed: bool,

    // publishedTracks that participant is publishing
    published_tracks: HashMap<livekit::TrackId, Arc<dyn types::MediaTrack>>,
    subscription_permission: Option<Arc<livekit::SubscriptionPermission>>,
    // subscriber permission for published tracks (both media and data)
    subscriber_permissions: Option<HashMap<livekit::ParticipantIdentity, livekit::TrackPermission>>, // subscriberIdentity => *livekit.TrackPermission

    // callbacks & handlers
    on_close: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl UpTrackManager {
    pub fn new(params: UpTrackManagerParams) -> Arc<UpTrackManager> {
        Arc::new_cyclic(|this| UpTrackManager {
            params,
            lock: RwLock::new(UpTrackManagerInner {
                subscription_permission_version: TimedVersion::default(),
                closed: false,
                published_tracks: HashMap::new(),
                subscription_permission: None,
                subscriber_permissions: None,
                on_close: None,
            }),
            on_track_updated: RwLock::new(None),
            this: this.clone(),
        })
    }

    pub fn close(&self, is_expected_to_resume: bool) {
        let mut u = self.lock.write();
        if u.closed {
            return;
        }

        u.closed = true;

        let published_tracks = std::mem::take(&mut u.published_tracks);
        drop(u);

        for t in published_tracks.values() {
            t.close(is_expected_to_resume);
        }

        if let Some(on_close) = self.get_on_up_track_manager_close() {
            on_close();
        }
    }

    pub fn on_up_track_manager_close(&self, f: impl Fn() + Send + Sync + 'static) {
        let mut u = self.lock.write();
        u.on_close = Some(Arc::new(f));
    }

    fn get_on_up_track_manager_close(&self) -> Option<Arc<dyn Fn() + Send + Sync>> {
        let u = self.lock.read();

        u.on_close.clone()
    }

    pub fn to_proto(&self) -> Vec<livekit::TrackInfo> {
        let u = self.lock.read();

        let mut track_infos = Vec::new();
        for t in u.published_tracks.values() {
            track_infos.push(t.to_proto());
        }

        track_infos
    }

    pub fn on_published_track_updated(
        &self,
        f: impl Fn(Arc<dyn types::MediaTrack>) + Send + Sync + 'static,
    ) {
        *self.on_track_updated.write() = Some(Arc::new(f));
    }

    pub fn set_published_track_muted(
        &self,
        track_id: &livekit::TrackId,
        muted: bool,
    ) -> (Option<Arc<dyn types::MediaTrack>>, bool) {
        let mut changed = false;
        let track = self.get_published_track(track_id);
        if let Some(track) = &track {
            let current_muted = track.is_muted();
            track.set_muted(muted);

            if current_muted != track.is_muted() {
                changed = true;
                debugw!(
                    self.params.logger,
                    "publisher mute status changed",
                    "trackID" => track_id,
                    "muted" => track.is_muted(),
                );
                let on_track_updated = self.on_track_updated.read().clone();
                if let Some(on_track_updated) = on_track_updated {
                    on_track_updated(track.clone());
                }
            }
        }

        (track, changed)
    }

    pub fn get_published_track(
        &self,
        track_id: &livekit::TrackId,
    ) -> Option<Arc<dyn types::MediaTrack>> {
        let u = self.lock.read();

        u.get_published_track_locked(track_id)
    }

    pub fn get_published_tracks(&self) -> Vec<Arc<dyn types::MediaTrack>> {
        let u = self.lock.read();

        u.published_tracks.values().cloned().collect()
    }

    #[allow(clippy::type_complexity)]
    pub fn update_subscription_permission(
        &self,
        subscription_permission: Option<&livekit::SubscriptionPermission>,
        timed_version: TimedVersion,
        resolver_by_sid: Option<
            Box<
                dyn Fn(&livekit::ParticipantId) -> Option<Arc<dyn types::LocalParticipant>>
                    + Send
                    + Sync,
            >,
        >,
    ) -> Result<(), Error> {
        let mut u = self.lock.write();
        if !timed_version.is_zero() {
            // it's possible for permission updates to come from another node. In that case
            // they would be the authority for this participant's permissions
            // we do not want to initialize subscriptionPermissionVersion too early since if another machine is the
            // owner for the data, we'd prefer to use their TimedVersion
            // ignore older version
            if !timed_version.after(u.subscription_permission_version) {
                debugw!(
                    self.params.logger,
                    "skipping older subscription permission version",
                    "existingValue" => u.subscription_permission,
                    "existingVersion" => u.subscription_permission_version,
                    "requestingValue" => subscription_permission,
                    "requestingVersion" => timed_version,
                );
                return Ok(());
            }
            u.subscription_permission_version.update(timed_version);
        } else {
            // for requests coming from the current node, use local versions
            let next = self.params.version_generator.next();
            u.subscription_permission_version.update(next);
        }

        // store as is for use when migrating
        u.subscription_permission = subscription_permission.map(|sp| Arc::new(sp.clone()));
        let Some(subscription_permission) = subscription_permission else {
            debugw!(
                self.params.logger,
                "updating subscription permission, setting to nil",
                "version" => u.subscription_permission_version,
            );
            // possible to get a nil when migrating
            return Ok(());
        };

        debugw!(
            self.params.logger,
            "updating subscription permission",
            "permissions" => u.subscription_permission,
            "version" => u.subscription_permission_version,
        );
        if let Err(err) = self.parse_subscription_permissions_locked(
            &mut u,
            subscription_permission,
            |u, p_id| {
                RwLockWriteGuard::unlocked(u, || {
                    let mut p = None;
                    if let Some(resolver_by_sid) = &resolver_by_sid {
                        p = resolver_by_sid(p_id);
                    }
                    p
                })
            },
        ) {
            // when failed, do not override previous permissions
            errorw!(
                self.params.logger,
                "failed updating subscription permission",
                err
            );
            return Err(err);
        }
        drop(u);

        self.maybe_revoke_subscriptions();

        Ok(())
    }

    pub fn subscription_permission(
        &self,
    ) -> (Option<Arc<livekit::SubscriptionPermission>>, TimedVersion) {
        let u = self.lock.read();

        if u.subscription_permission_version.is_zero() {
            return (None, u.subscription_permission_version.load());
        }

        (
            u.subscription_permission.clone(),
            u.subscription_permission_version.load(),
        )
    }

    pub fn has_permission(
        &self,
        track_id: &livekit::TrackId,
        sub_identity: &livekit::ParticipantIdentity,
    ) -> bool {
        let u = self.lock.read();

        u.has_permission_locked(track_id, sub_identity)
    }

    pub fn get_allowed_subscribers(
        &self,
        track_id: &livekit::TrackId,
    ) -> Option<Vec<livekit::ParticipantIdentity>> {
        let u = self.lock.read();

        u.get_allowed_subscribers_locked(track_id)
    }

    pub fn update_published_audio_track(
        &self,
        update: &livekit::UpdateLocalAudioTrack,
    ) -> Option<Arc<dyn types::MediaTrack>> {
        let track = self.get_published_track(&livekit::TrackId::from(&update.track_sid));
        if let Some(track) = &track {
            track.update_audio_track(update);
            let on_track_updated = self.on_track_updated.read().clone();
            if let Some(on_track_updated) = on_track_updated {
                on_track_updated(track.clone());
            }
        }

        track
    }

    pub fn update_published_video_track(
        &self,
        update: &livekit::UpdateLocalVideoTrack,
    ) -> Option<Arc<dyn types::MediaTrack>> {
        let track = self.get_published_track(&livekit::TrackId::from(&update.track_sid));
        if let Some(track) = &track {
            track.update_video_track(update);
            let on_track_updated = self.on_track_updated.read().clone();
            if let Some(on_track_updated) = on_track_updated {
                on_track_updated(track.clone());
            }
        }

        track
    }

    pub fn add_published_track(&self, track: Arc<dyn types::MediaTrack>) {
        {
            let mut u = self.lock.write();
            u.published_tracks
                .entry(track.id())
                .or_insert_with(|| track.clone());
        }
        debugw!(
            self.params.logger,
            "added published track",
            "trackID" => track.id(),
            "trackInfo" => track.to_proto(),
        );

        let this = self.this.clone();
        let weak_track = Arc::downgrade(&track);
        track.add_on_close(Some(Box::new(move |_is_expected_to_resume| {
            let (Some(u), Some(track)) = (this.upgrade(), weak_track.upgrade()) else {
                return;
            };
            let mut u = u.lock.write();
            u.published_tracks.remove(&track.id());
            // not modifying subscription permissions, will get reset on next update from participant
        })));
    }

    pub fn remove_published_track(
        &self,
        track: Arc<dyn types::MediaTrack>,
        is_expected_to_resume: bool,
    ) {
        track.close(is_expected_to_resume);

        let mut u = self.lock.write();
        u.published_tracks.remove(&track.id());
    }

    fn parse_subscription_permissions_locked(
        &self,
        u: &mut RwLockWriteGuard<'_, UpTrackManagerInner>,
        subscription_permission: &livekit::SubscriptionPermission,
        resolver: impl Fn(
            &mut RwLockWriteGuard<'_, UpTrackManagerInner>,
            &livekit::ParticipantId,
        ) -> Option<Arc<dyn types::LocalParticipant>>,
    ) -> Result<(), Error> {
        // every update overrides the existing

        // all_participants takes precedence
        if subscription_permission.all_participants {
            // everything is allowed, nothing else to do
            u.subscriber_permissions = None;
            return Ok(());
        }

        // per participant permissions
        let mut subscriber_permissions = HashMap::new();
        for track_perms in &subscription_permission.track_permissions {
            let mut subscriber_identity =
                livekit::ParticipantIdentity::from(&track_perms.participant_identity);
            if subscriber_identity.is_empty() {
                if track_perms.participant_sid.is_empty() {
                    return Err(Error::SubscriptionPermissionNeedsId);
                }

                let Some(sub) = resolver(
                    u,
                    &livekit::ParticipantId::from(&track_perms.participant_sid),
                ) else {
                    warnw!(
                        self.params.logger,
                        "could not find subscriber for permissions update", nil,
                        "subscriberID" => track_perms.participant_sid,
                        "subscriptionPermission" => subscription_permission,
                    );
                    continue;
                };

                subscriber_identity = sub.identity();
            } else if !track_perms.participant_sid.is_empty() {
                let sub = resolver(
                    u,
                    &livekit::ParticipantId::from(&track_perms.participant_sid),
                );
                if let Some(sub) = &sub
                    && sub.identity() != subscriber_identity
                {
                    errorw!(
                        self.params.logger,
                        "participant identity mismatch", nil,
                        "expected" => subscriber_identity,
                        "got" => sub.identity(),
                        "subscriptionPermission" => subscription_permission,
                    );
                }
                if sub.is_none() {
                    warnw!(
                        self.params.logger,
                        "could not find subscriber for permissions update", nil,
                        "subscriberID" => track_perms.participant_sid,
                        "subscriptionPermission" => subscription_permission,
                    );
                }
            }

            subscriber_permissions.insert(subscriber_identity, track_perms.clone());
        }

        u.subscriber_permissions = Some(subscriber_permissions);

        Ok(())
    }

    fn maybe_revoke_subscriptions(&self) {
        let u = self.lock.write();

        for (track_id, track) in &u.published_tracks {
            let Some(allowed) = u.get_allowed_subscribers_locked(track_id) else {
                // no restrictions
                continue;
            };

            track.revoke_disallowed_subscribers(&allowed);
        }
    }

    pub fn debug_info(&self) -> sfu::DebugInfo {
        let mut info = sfu::DebugInfo::new();
        let mut published_track_info = serde_json::Map::new();

        {
            let u = self.lock.read();
            for (track_id, track) in &u.published_tracks {
                // Not here yet: `if mt, ok := track.(*MediaTrack); ok`, which puts
                // `mt.DebugInfo()` here. `mediatrack.go` is not ported.
                published_track_info.insert(
                    track_id.to_string(),
                    serde_json::json!({
                        "ID": track.id().as_str(),
                        "Kind": track.kind().as_str_name(),
                        "PubMuted": track.is_muted(),
                    }),
                );
            }
        }

        info.insert(
            "PublishedTracks".to_string(),
            serde_json::Value::Object(published_track_info),
        );

        info
    }

    pub fn get_audio_level(&self) -> (f64, bool) {
        let mut level = 0.0;
        let mut active = false;
        for pt in self.get_published_tracks() {
            if pt.source() == livekit::TrackSource::Microphone {
                let (tl, ta) = pt.get_audio_level();
                if ta {
                    active = true;
                    if tl > level {
                        level = tl;
                    }
                }
            }
        }
        (level, active)
    }
}

impl UpTrackManagerInner {
    fn get_published_track_locked(
        &self,
        track_id: &livekit::TrackId,
    ) -> Option<Arc<dyn types::MediaTrack>> {
        self.published_tracks.get(track_id).cloned()
    }

    fn has_permission_locked(
        &self,
        track_id: &livekit::TrackId,
        subscriber_identity: &livekit::ParticipantIdentity,
    ) -> bool {
        let Some(subscriber_permissions) = &self.subscriber_permissions else {
            return true;
        };

        let Some(perms) = subscriber_permissions.get(subscriber_identity) else {
            return false;
        };

        if perms.all_tracks {
            return true;
        }

        for sid in &perms.track_sids {
            if track_id == sid.as_str() {
                return true;
            }
        }

        false
    }

    // returns a list of participants that are allowed to subscribe to the track. if nil is returned, it means everyone is
    // allowed to subscribe to this track
    fn get_allowed_subscribers_locked(
        &self,
        track_id: &livekit::TrackId,
    ) -> Option<Vec<livekit::ParticipantIdentity>> {
        let subscriber_permissions = self.subscriber_permissions.as_ref()?;

        let mut allowed = Vec::new();
        for (subscriber_identity, perms) in subscriber_permissions {
            if perms.all_tracks {
                allowed.push(subscriber_identity.clone());
                continue;
            }

            for sid in &perms.track_sids {
                if track_id == sid.as_str() {
                    allowed.push(subscriber_identity.clone());
                    break;
                }
            }
        }

        Some(allowed)
    }
}

#[cfg(test)]
#[path = "uptrackmanager_test.rs"]
mod uptrackmanager_test;
