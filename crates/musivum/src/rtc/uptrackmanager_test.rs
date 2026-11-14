// Ported from LiveKit, pkg/rtc/uptrackmanager_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks; `defaultUptrackManagerParams`
// is a function, as the params are not copied; what the tests read and write of the
// manager's fields is behind its lock, which `has_permission_locked` is called with as well.

use super::*;

use crate::protocol::logger;
use crate::protocol::utils;
use crate::rtc::types::{FakeLocalParticipant, FakeMediaTrack};

fn default_uptrack_manager_params() -> UpTrackManagerParams {
    UpTrackManagerParams {
        logger: logger::get_logger(),
        version_generator: utils::new_default_timed_version_generator(),
    }
}

fn fake_media_track(id: &str) -> Arc<FakeMediaTrack> {
    let tr = Arc::new(FakeMediaTrack::default());
    tr.id_returns(id.into());
    tr
}

#[allow(clippy::type_complexity)]
fn new_sid_resolver(
    by_sid: [(&'static str, Arc<FakeLocalParticipant>); 2],
) -> Option<
    Box<dyn Fn(&livekit::ParticipantId) -> Option<Arc<dyn types::LocalParticipant>> + Send + Sync>,
> {
    Some(Box::new(move |sid| {
        for (s, lp) in &by_sid {
            if sid == s {
                let lp: Arc<dyn types::LocalParticipant> = lp.clone();
                return Some(lp);
            }
        }

        None
    }))
}

fn subscriber_permissions(
    um: &UpTrackManager,
) -> Option<HashMap<livekit::ParticipantIdentity, livekit::TrackPermission>> {
    um.lock.read().subscriber_permissions.clone()
}

#[test]
fn test_update_subscription_permission() {
    // t.Run("updates subscription permission")
    {
        let um = UpTrackManager::new(default_uptrack_manager_params());
        let vg = utils::new_default_timed_version_generator();

        let tra = fake_media_track("audio");
        um.lock
            .write()
            .published_tracks
            .insert("audio".into(), tra.clone());

        let trv = fake_media_track("video");
        um.lock
            .write()
            .published_tracks
            .insert("video".into(), trv.clone());

        // no restrictive subscription permission
        let subscription_permission = livekit::SubscriptionPermission {
            all_participants: true,
            ..Default::default()
        };
        um.update_subscription_permission(Some(&subscription_permission), vg.next(), None)
            .unwrap();
        assert!(subscriber_permissions(&um).is_none());

        // nobody is allowed to subscribe
        let subscription_permission = livekit::SubscriptionPermission {
            track_permissions: vec![],
            ..Default::default()
        };
        um.update_subscription_permission(Some(&subscription_permission), vg.next(), None)
            .unwrap();
        assert!(subscriber_permissions(&um).is_some());
        assert_eq!(0, subscriber_permissions(&um).unwrap().len());

        let lp1 = Arc::new(FakeLocalParticipant::default());
        lp1.identity_returns("p1".into());
        let lp2 = Arc::new(FakeLocalParticipant::default());
        lp2.identity_returns("p2".into());

        let sid_resolver = new_sid_resolver([("p1", lp1.clone()), ("p2", lp2.clone())]);

        // allow all tracks for participants
        let perms1 = livekit::TrackPermission {
            participant_sid: "p1".to_string(),
            all_tracks: true,
            ..Default::default()
        };
        let perms2 = livekit::TrackPermission {
            participant_sid: "p2".to_string(),
            all_tracks: true,
            ..Default::default()
        };
        let subscription_permission = livekit::SubscriptionPermission {
            track_permissions: vec![perms1.clone(), perms2.clone()],
            ..Default::default()
        };
        um.update_subscription_permission(Some(&subscription_permission), vg.next(), sid_resolver)
            .unwrap();
        let got = subscriber_permissions(&um).unwrap();
        assert_eq!(2, got.len());
        assert_eq!(Some(&perms1), got.get(&"p1".into()));
        assert_eq!(Some(&perms2), got.get(&"p2".into()));

        // allow all tracks for some and restrictive for others
        let perms1 = livekit::TrackPermission {
            participant_identity: "p1".to_string(),
            all_tracks: true,
            ..Default::default()
        };
        let perms2 = livekit::TrackPermission {
            participant_identity: "p2".to_string(),
            track_sids: vec!["audio".to_string()],
            ..Default::default()
        };
        let perms3 = livekit::TrackPermission {
            participant_identity: "p3".to_string(),
            track_sids: vec!["video".to_string()],
            ..Default::default()
        };
        let subscription_permission = livekit::SubscriptionPermission {
            track_permissions: vec![perms1.clone(), perms2.clone(), perms3.clone()],
            ..Default::default()
        };
        um.update_subscription_permission(Some(&subscription_permission), vg.next(), None)
            .unwrap();
        let got = subscriber_permissions(&um).unwrap();
        assert_eq!(3, got.len());
        assert_eq!(Some(&perms1), got.get(&"p1".into()));
        assert_eq!(Some(&perms2), got.get(&"p2".into()));
        assert_eq!(Some(&perms3), got.get(&"p3".into()));
    }

    // t.Run("updates subscription permission using both")
    {
        let um = UpTrackManager::new(default_uptrack_manager_params());
        let vg = utils::new_default_timed_version_generator();

        let tra = fake_media_track("audio");
        um.lock
            .write()
            .published_tracks
            .insert("audio".into(), tra.clone());

        let trv = fake_media_track("video");
        um.lock
            .write()
            .published_tracks
            .insert("video".into(), trv.clone());

        let lp1 = Arc::new(FakeLocalParticipant::default());
        lp1.identity_returns("p1".into());
        let lp2 = Arc::new(FakeLocalParticipant::default());
        lp2.identity_returns("p2".into());

        let sid_resolver = new_sid_resolver([("p1", lp1.clone()), ("p2", lp2.clone())]);

        // allow all tracks for participants
        let perms1 = livekit::TrackPermission {
            participant_sid: "p1".to_string(),
            participant_identity: "p1".to_string(),
            all_tracks: true,
            ..Default::default()
        };
        let perms2 = livekit::TrackPermission {
            participant_sid: "p2".to_string(),
            participant_identity: "p2".to_string(),
            all_tracks: true,
            ..Default::default()
        };
        let subscription_permission = livekit::SubscriptionPermission {
            track_permissions: vec![perms1.clone(), perms2.clone()],
            ..Default::default()
        };
        let err = um.update_subscription_permission(
            Some(&subscription_permission),
            vg.next(),
            sid_resolver,
        );
        assert_eq!(Ok(()), err);
        let got = subscriber_permissions(&um).unwrap();
        assert_eq!(2, got.len());
        assert_eq!(Some(&perms1), got.get(&"p1".into()));
        assert_eq!(Some(&perms2), got.get(&"p2".into()));

        // mismatched identities should fail a permission update
        let bad_sid_resolver = new_sid_resolver([("p1", lp2.clone()), ("p2", lp1.clone())]);

        let err = um.update_subscription_permission(
            Some(&subscription_permission),
            vg.next(),
            bad_sid_resolver,
        );
        assert_eq!(Ok(()), err);
        let got = subscriber_permissions(&um).unwrap();
        assert_eq!(2, got.len());
        assert_eq!(Some(&perms1), got.get(&"p1".into()));
        assert_eq!(Some(&perms2), got.get(&"p2".into()));
    }

    // t.Run("update versions")
    {
        let um = UpTrackManager::new(default_uptrack_manager_params());
        let vg = utils::new_default_timed_version_generator();

        let (v0, v1, v2) = (vg.next(), vg.next(), vg.next());

        um.update_subscription_permission(
            Some(&livekit::SubscriptionPermission::default()),
            v1,
            None,
        )
        .unwrap();
        assert_eq!(
            v1.load(),
            um.lock.read().subscription_permission_version.load(),
            "first update should be applied"
        );

        um.update_subscription_permission(
            Some(&livekit::SubscriptionPermission::default()),
            v2,
            None,
        )
        .unwrap();
        assert_eq!(
            v2.load(),
            um.lock.read().subscription_permission_version.load(),
            "ordered updates should be applied"
        );

        um.update_subscription_permission(
            Some(&livekit::SubscriptionPermission::default()),
            v0,
            None,
        )
        .unwrap();
        assert_eq!(
            v2.load(),
            um.lock.read().subscription_permission_version.load(),
            "out of order updates should be ignored"
        );

        um.update_subscription_permission(
            Some(&livekit::SubscriptionPermission::default()),
            utils::TimedVersion(0),
            None,
        )
        .unwrap();
        assert!(
            um.lock.read().subscription_permission_version.after(v2),
            "zero version in updates should use next local version"
        );
    }
}

#[test]
fn test_subscription_permission() {
    // t.Run("checks subscription permission")
    {
        let um = UpTrackManager::new(default_uptrack_manager_params());
        let vg = utils::new_default_timed_version_generator();

        let has_permission_locked = |track_id: &str, subscriber_identity: &str| {
            um.lock
                .read()
                .has_permission_locked(&track_id.into(), &subscriber_identity.into())
        };

        let tra = fake_media_track("audio");
        um.lock
            .write()
            .published_tracks
            .insert("audio".into(), tra.clone());

        let trv = fake_media_track("video");
        um.lock
            .write()
            .published_tracks
            .insert("video".into(), trv.clone());

        // no restrictive permission
        let subscription_permission = livekit::SubscriptionPermission {
            all_participants: true,
            ..Default::default()
        };
        um.update_subscription_permission(Some(&subscription_permission), vg.next(), None)
            .unwrap();
        assert!(has_permission_locked("audio", "p1"));
        assert!(has_permission_locked("audio", "p2"));

        // nobody is allowed to subscribe
        let subscription_permission = livekit::SubscriptionPermission {
            track_permissions: vec![],
            ..Default::default()
        };
        um.update_subscription_permission(Some(&subscription_permission), vg.next(), None)
            .unwrap();
        assert!(!has_permission_locked("audio", "p1"));
        assert!(!has_permission_locked("audio", "p2"));

        // allow all tracks for participants
        let subscription_permission = livekit::SubscriptionPermission {
            track_permissions: vec![
                livekit::TrackPermission {
                    participant_identity: "p1".to_string(),
                    all_tracks: true,
                    ..Default::default()
                },
                livekit::TrackPermission {
                    participant_identity: "p2".to_string(),
                    all_tracks: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        um.update_subscription_permission(Some(&subscription_permission), vg.next(), None)
            .unwrap();
        assert!(has_permission_locked("audio", "p1"));
        assert!(has_permission_locked("video", "p1"));
        assert!(has_permission_locked("audio", "p2"));
        assert!(has_permission_locked("video", "p2"));

        // add a new track after permissions are set
        let trs = fake_media_track("screen");
        um.lock
            .write()
            .published_tracks
            .insert("screen".into(), trs.clone());

        assert!(has_permission_locked("audio", "p1"));
        assert!(has_permission_locked("video", "p1"));
        assert!(has_permission_locked("screen", "p1"));
        assert!(has_permission_locked("audio", "p2"));
        assert!(has_permission_locked("video", "p2"));
        assert!(has_permission_locked("screen", "p2"));

        // allow all tracks for some and restrictive for others
        let subscription_permission = livekit::SubscriptionPermission {
            track_permissions: vec![
                livekit::TrackPermission {
                    participant_identity: "p1".to_string(),
                    all_tracks: true,
                    ..Default::default()
                },
                livekit::TrackPermission {
                    participant_identity: "p2".to_string(),
                    track_sids: vec!["audio".to_string()],
                    ..Default::default()
                },
                livekit::TrackPermission {
                    participant_identity: "p3".to_string(),
                    track_sids: vec!["video".to_string()],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        um.update_subscription_permission(Some(&subscription_permission), vg.next(), None)
            .unwrap();
        assert!(has_permission_locked("audio", "p1"));
        assert!(has_permission_locked("video", "p1"));
        assert!(has_permission_locked("screen", "p1"));

        assert!(has_permission_locked("audio", "p2"));
        assert!(!has_permission_locked("video", "p2"));
        assert!(!has_permission_locked("screen", "p2"));

        assert!(!has_permission_locked("audio", "p3"));
        assert!(has_permission_locked("video", "p3"));
        assert!(!has_permission_locked("screen", "p3"));

        // add a new track after restrictive permissions are set
        let trw = fake_media_track("watch");
        um.lock
            .write()
            .published_tracks
            .insert("watch".into(), trw.clone());

        assert!(has_permission_locked("audio", "p1"));
        assert!(has_permission_locked("video", "p1"));
        assert!(has_permission_locked("screen", "p1"));
        assert!(has_permission_locked("watch", "p1"));

        assert!(has_permission_locked("audio", "p2"));
        assert!(!has_permission_locked("video", "p2"));
        assert!(!has_permission_locked("screen", "p2"));
        assert!(!has_permission_locked("watch", "p2"));

        assert!(!has_permission_locked("audio", "p3"));
        assert!(has_permission_locked("video", "p3"));
        assert!(!has_permission_locked("screen", "p3"));
        assert!(!has_permission_locked("watch", "p3"));
    }
}
