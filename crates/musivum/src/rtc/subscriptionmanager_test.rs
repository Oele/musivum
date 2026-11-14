// Ported from LiveKit, pkg/rtc/subscriptionmanager_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks.
// - `init()`, which shortens three times for every test of the package, is the three
//   constants at the top, which `subscriptionmanager.rs` uses in a test build.
// - The original's tests replace fields of `sm.params` after the manager is made. The params
//   cannot change here, so a test manager (`TestSubscriptionManager`) has params that pass
//   each call on to what is in a slot of `TestParams`, and `sm.set_track_resolver(..)`
//   stands for `sm.params.TrackResolver = ..`. The pacer and the reconcile interval, which
//   are no callbacks, are given when the manager is made (`TestSubscriptionParams` has them
//   beside the two limits). The test manager also holds the fake participant, which the
//   manager holds weakly, and closes the manager when it goes out of scope (`defer
//   sm.Close(false)`). The data track resolver, nil in the original where a test subscribes
//   no data track, resolves nothing.
// - The fakes do not record a callback (see the header of `types/interfaces.rs`), and these
//   tests call the callbacks that the manager registered (`fst.OnCloseArgsForCall(0)(..)`).
//   A fake subscribed track or data down track whose callbacks a test calls is made by
//   `new_fake_subscribed_track` or `new_fake_data_down_track`: their stubs keep the
//   callbacks in a table of this file, under the address of the fake.
// - Settings and options are copied where the original keeps a pointer, so "the same
//   settings" is equality where the original compares pointers.
// - `testResolver.Resolve(nil, ..)` is called with the fake participant: the resolver takes
//   a participant, not an optional one.
// - `require.Eventually` is `eventually`, at the end of the tests: `eventually_async` with
//   the two times of this file, after one check interval, since testify looks for the first
//   time after one tick and `eventually_async` at once ("needs subscribe did not persist
//   across track close" is about what the worker has done by then). Its message is the
//   comment after the call.
// - The goroutines of `TestConcurrentFirstSubscribe` are tasks that start together.

use std::sync::LazyLock;

use super::*;
use crate::go::time::HOUR;
use crate::protocol::logger;
use crate::rtc::types::{
    DataTrack, FakeDataDownTrack, FakeDataTrack, FakeLocalParticipant, FakeMediaTrack,
    FakeParticipantTelemetryListener, FakeSubscribedTrack, Participant,
};
use crate::testutils::eventually_async;
use crate::testutils::fakes;

// func init()
pub(super) const RECONCILE_INTERVAL: Duration = Duration::from_millis(50);
pub(super) const NOT_FOUND_TIMEOUT: Duration = Duration::from_millis(200);
pub(super) const SUBSCRIPTION_TIMEOUT: Duration = Duration::from_millis(200);

const SUB_SETTLE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(600);
const SUB_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe() {
    {
        let name = "happy path subscribe";
        let sm = new_test_subscription_manager();
        let resolver = new_test_resolver(true, true, "pub", "pubID");
        sm.set_track_resolver({
            let resolver = resolver.clone();
            move |sub, track_id| resolver.resolve(sub, track_id)
        });
        let sub_count = Arc::new(atomic::Int32::default());
        let failed = Arc::new(atomic::Bool::default());
        sm.set_on_track_subscribed({
            let sub_count = sub_count.clone();
            move |_sub_track| {
                sub_count.add(1);
            }
        });
        sm.set_on_subscription_error({
            let failed = failed.clone();
            move |_track_id, _fatal, _err| {
                failed.store(true);
            }
        });
        let num_participant_subscribed = Arc::new(atomic::Int32::default());
        let num_participant_unsubscribed = Arc::new(atomic::Int32::default());
        sm.on_subscribe_status_changed(Box::new({
            let num_participant_subscribed = num_participant_subscribed.clone();
            let num_participant_unsubscribed = num_participant_unsubscribed.clone();
            move |_pub_id, subscribed| {
                if subscribed {
                    num_participant_subscribed.add(1);
                } else {
                    num_participant_unsubscribed.add(1);
                }
            }
        }));

        sm.subscribe_to_track(&"track".into(), false);
        let s = sm.subscription("track");
        assert!(s.is_desired(), "{name}");
        eventually(|| sub_count.load() == 1).await; // track was not subscribed

        assert!(s.get_subscribed_track().is_some(), "{name}");
        assert_eq!(1, sm.get_subscribed_tracks().len(), "{name}");

        eventually(|| sm.get_subscribed_participants().len() == 1).await; // GetSubscribedParticipants should have returned one item
        assert_eq!(
            "pubID",
            sm.get_subscribed_participants()[0].as_str(),
            "{name}"
        );

        // ensure telemetry events are sent
        let tl = sm.telemetry_listener();
        assert_eq!(1, tl.on_track_subscribe_requested_call_count(), "{name}");

        // ensure bound
        set_test_subscribed_track_bound(s.get_subscribed_track().unwrap());
        eventually(|| !s.needs_bind()).await; // track was not bound

        // telemetry event should have been sent
        assert_eq!(1, tl.on_track_subscribed_call_count(), "{name}");

        time::sleep(NOT_FOUND_TIMEOUT).await;
        assert!(!failed.load(), "{name}");

        resolver.set_pause(true);
        // ensure its resilience after being closed
        set_test_subscribed_track_closed(s.get_subscribed_track().unwrap(), false);
        eventually(|| s.needs_subscribe()).await; // needs subscribe did not persist across track close
        resolver.set_pause(false);

        eventually(|| s.is_desired() && !s.needs_subscribe()).await; // track was not resubscribed

        // was subscribed twice, unsubscribed once (due to close)
        eventually(|| num_participant_subscribed.load() == 2).await; // participant subscribe status was not updated twice
        eventually(|| num_participant_unsubscribed.load() == 1).await; // participant unsubscribe status was not updated
    }

    {
        let name = "no track permission";
        let sm = new_test_subscription_manager();
        let resolver = new_test_resolver(false, true, "pub", "pubID");
        sm.set_track_resolver({
            let resolver = resolver.clone();
            move |sub, track_id| resolver.resolve(sub, track_id)
        });
        let failed = Arc::new(atomic::Bool::default());
        sm.set_on_subscription_error({
            let failed = failed.clone();
            move |_track_id, _fatal, _err| {
                failed.store(true);
            }
        });

        sm.subscribe_to_track(&"track".into(), false);
        let s = sm.subscription("track");
        eventually(|| !s.get_has_permission()).await; // should not have permission to subscribe

        time::sleep(SUBSCRIPTION_TIMEOUT).await;

        // should not have called failed callbacks, isDesired remains unchanged
        assert!(s.is_desired(), "{name}");
        assert!(!failed.load(), "{name}");
        assert!(s.needs_subscribe(), "{name}");
        assert_eq!(0, sm.get_subscribed_tracks().len(), "{name}");

        // trackSubscribed telemetry not sent
        let tl = sm.telemetry_listener();
        assert_eq!(1, tl.on_track_subscribe_requested_call_count(), "{name}");
        assert_eq!(0, tl.on_track_subscribed_call_count(), "{name}");

        // give permissions now
        resolver.lock.lock().has_permission = true;

        eventually(|| !s.needs_subscribe()).await; // should be subscribed

        assert_eq!(1, sm.get_subscribed_tracks().len(), "{name}");
    }

    {
        // t.Run("publisher left")
        let sm = new_test_subscription_manager();
        let resolver = new_test_resolver(true, true, "pub", "pubID");
        sm.set_track_resolver({
            let resolver = resolver.clone();
            move |sub, track_id| resolver.resolve(sub, track_id)
        });
        let failed = Arc::new(atomic::Bool::default());
        sm.set_on_subscription_error({
            let failed = failed.clone();
            move |_track_id, _fatal, _err| {
                failed.store(true);
            }
        });

        sm.subscribe_to_track(&"track".into(), false);
        let s = sm.subscription("track");
        eventually(|| !s.needs_subscribe()).await; // should be subscribed

        resolver.lock.lock().has_track = false;

        // publisher triggers close
        set_test_subscribed_track_closed(s.get_subscribed_track().unwrap(), false);

        eventually(|| !s.is_desired()).await; // isDesired not set to false
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unsubscribe() {
    let sm = new_test_subscription_manager();
    let unsub_count = Arc::new(atomic::Int32::default());
    sm.set_on_track_unsubscribed({
        let unsub_count = unsub_count.clone();
        move |_sub_track| {
            unsub_count.add(1);
        }
    });

    let resolver = new_test_resolver(true, true, "pub", "pubID");

    let s = Arc::new(MediaTrackSubscription {
        track_subscription: TrackSubscription {
            track_id: "track".into(),
            subscriber_id: sm.participant.id(),
            logger: logger::get_logger(),
            lock: RwLock::new(TrackSubscriptionInner {
                desired: true,
                publisher_id: "pubID".into(),
                publisher_identity: "pub".into(),
                has_permission: true,
                outer: MediaTrackSubscriptionInner {
                    bound: true,
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    });
    // a bunch of unfortunate manual wiring
    let res = resolver.resolve(sm.participant.clone(), &s.track_id);
    let track_changed_notifier = res.track_changed_notifier.clone().unwrap();
    track_changed_notifier.add_observer(sm.participant.id().as_str(), Box::new(|| {}));
    s.lock.write().changed_notifier = Some(track_changed_notifier.clone());
    let track = res.track.clone().unwrap();
    let st = track
        .add_subscriber(sm.participant.clone())
        .expect("no error")
        .unwrap();
    s.lock.write().outer.subscribed_track = Some(st.clone());
    st.on_close(Some(Box::new({
        let sm = sm.sm.clone();
        let s = s.clone();
        move |is_expected_to_resume| {
            sm.handle_subscribed_track_close(&s, is_expected_to_resume);
        }
    })));
    fakes::downcast::<FakeMediaTrack>(track).remove_subscriber_calls({
        let st = st.clone();
        move |_p_id, is_expected_to_resume| {
            set_test_subscribed_track_closed(st.clone(), is_expected_to_resume);
        }
    });

    sm.lock
        .write()
        .subscriptions
        .insert("track".into(), s.clone());

    assert!(!s.needs_subscribe());
    assert!(!s.needs_unsubscribe());

    // unsubscribe
    sm.unsubscribe_from_track(&"track".into());
    assert!(!s.is_desired());

    eventually(|| {
        if s.needs_unsubscribe() {
            return false;
        }
        if sm.pending_unsubscribes.load() != 0 {
            return false;
        }
        let sub_len = sm.lock.read().subscriptions.len();
        sub_len == 0
    })
    .await; // Track was not unsubscribed

    // no traces should be left
    assert_eq!(0, sm.get_subscribed_tracks().len());
    // the observer is dropped on a goroutine of its own
    eventually(|| !track_changed_notifier.has_observers()).await; // observer was not removed

    let tl = sm.telemetry_listener();
    assert_eq!(1, tl.on_track_unsubscribed_call_count());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_status_changed() {
    let sm = new_test_subscription_manager();
    let resolver = new_test_resolver(true, true, "pub", "pubID");
    sm.set_track_resolver({
        let resolver = resolver.clone();
        move |sub, track_id| resolver.resolve(sub, track_id)
    });
    let num_participant_subscribed = Arc::new(atomic::Int32::default());
    let num_participant_unsubscribed = Arc::new(atomic::Int32::default());
    sm.on_subscribe_status_changed(Box::new({
        let num_participant_subscribed = num_participant_subscribed.clone();
        let num_participant_unsubscribed = num_participant_unsubscribed.clone();
        move |_pub_id, subscribed| {
            if subscribed {
                num_participant_subscribed.add(1);
            } else {
                num_participant_unsubscribed.add(1);
            }
        }
    }));

    sm.subscribe_to_track(&"track1".into(), false);
    sm.subscribe_to_track(&"track2".into(), false);
    let s1 = sm.subscription("track1");
    let s2 = sm.subscription("track2");
    eventually(|| !s1.needs_subscribe() && !s2.needs_subscribe()).await; // track1 and track2 should be subscribed
    let st1 = s1.get_subscribed_track().unwrap();
    st1.on_close(Some(Box::new({
        let sm = sm.sm.clone();
        let s1 = s1.clone();
        move |is_expected_to_resume| {
            sm.handle_subscribed_track_close(&s1, is_expected_to_resume);
        }
    })));
    let st2 = s2.get_subscribed_track().unwrap();
    st2.on_close(Some(Box::new({
        let sm = sm.sm.clone();
        let s2 = s2.clone();
        move |is_expected_to_resume| {
            sm.handle_subscribed_track_close(&s2, is_expected_to_resume);
        }
    })));
    fakes::downcast::<FakeMediaTrack>(st1.media_track().unwrap()).remove_subscriber_calls({
        let st1 = st1.clone();
        move |_p_id, is_expected_to_resume| {
            set_test_subscribed_track_closed(st1.clone(), is_expected_to_resume);
        }
    });
    fakes::downcast::<FakeMediaTrack>(st2.media_track().unwrap()).remove_subscriber_calls({
        let st2 = st2.clone();
        move |_p_id, is_expected_to_resume| {
            set_test_subscribed_track_closed(st2.clone(), is_expected_to_resume);
        }
    });

    eventually(|| num_participant_subscribed.load() == 1).await; // should be subscribed to publisher
    assert_eq!(0, num_participant_unsubscribed.load());
    assert!(sm.is_subscribed_to(&"pubID".into()));

    // now unsubscribe track2, no event should be fired
    sm.unsubscribe_from_track(&"track2".into());
    eventually(|| !s2.needs_unsubscribe()).await; // track2 should be unsubscribed
    assert_eq!(0, num_participant_unsubscribed.load());

    // unsubscribe track1, expect event
    sm.unsubscribe_from_track(&"track1".into());
    eventually(|| !s1.needs_unsubscribe()).await; // track1 should be unsubscribed
    eventually(|| num_participant_unsubscribed.load() == 1).await; // should be subscribed to publisher
    assert!(!sm.is_subscribed_to(&"pubID".into()));
}

// clients may send update subscribed settings prior to subscription events coming through
// settings should be persisted and used when the subscription does take place.
#[tokio::test(flavor = "multi_thread")]
async fn test_update_settings_before_subscription() {
    let sm = new_test_subscription_manager();
    let resolver = new_test_resolver(true, true, "pub", "pubID");
    sm.set_track_resolver({
        let resolver = resolver.clone();
        move |sub, track_id| resolver.resolve(sub, track_id)
    });

    let settings = livekit::UpdateTrackSettings {
        disabled: true,
        width: 100,
        height: 100,
        ..Default::default()
    };
    sm.update_subscribed_track_settings(&"track".into(), &settings);

    sm.subscribe_to_track(&"track".into(), false);

    let s = sm.subscription("track");
    eventually(|| !s.needs_subscribe()).await; // Track should be subscribed

    let st = fakes::downcast::<FakeSubscribedTrack>(s.get_subscribed_track().unwrap());
    eventually(|| st.update_subscriber_settings_call_count() == 1).await; // UpdateSubscriberSettings should be called once

    let (applied, _) = st.update_subscriber_settings_args_for_call(0);
    assert_eq!(settings.disabled, applied.disabled);
    assert_eq!(settings.width, applied.width);
    assert_eq!(settings.height, applied.height);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_first_subscribe() {
    let settings = livekit::UpdateTrackSettings {
        width: 100,
        height: 100,
        ..Default::default()
    };
    for _ in 0..200 {
        let sm = new_test_subscription_manager();

        let lock = Arc::new(Mutex::new(false)); // subscribed
        let mt = Arc::new(FakeMediaTrack::default());
        mt.id_returns("track".into());
        mt.add_subscriber_calls({
            // the fake keeps its stubs, and a stub that held the fake would keep it alive
            let mt = Arc::downgrade(&mt);
            move |_| {
                let mut subscribed = lock.lock();
                if *subscribed {
                    return Err(Error::AlreadySubscribed);
                }
                *subscribed = true;
                let st = Arc::new(FakeSubscribedTrack::default());
                st.id_returns("track".into());
                st.media_track_returns(mt.upgrade().map(|mt| mt as Arc<dyn MediaTrack>));
                Ok(Some(st as Arc<dyn SubscribedTrack>))
            }
        });
        sm.set_track_resolver(move |_, _| MediaResolverResult {
            track: Some(mt.clone()),
            has_permission: true,
            publisher_id: "pubID".into(),
            publisher_identity: "pub".into(),
            track_changed_notifier: Some(crate::utils::ChangeNotifier::new()),
            track_removed_notifier: Some(crate::utils::ChangeNotifier::new()),
        });

        // two first subscribes and a settings update race to create the subscription
        let start = Arc::new(tokio::sync::Barrier::new(3));
        let mut done = Vec::new();
        let fs: [Box<dyn FnOnce() + Send>; 3] = [
            Box::new({
                let sm = sm.sm.clone();
                move || sm.subscribe_to_track(&"track".into(), false)
            }),
            Box::new({
                let sm = sm.sm.clone();
                move || sm.subscribe_to_track(&"track".into(), false)
            }),
            Box::new({
                let sm = sm.sm.clone();
                let settings = settings.clone();
                move || sm.update_subscribed_track_settings(&"track".into(), &settings)
            }),
        ];
        for f in fs {
            let start = start.clone();
            done.push(tokio::spawn(async move {
                start.wait().await;
                f();
            }));
        }
        for d in done {
            d.await.unwrap();
        }

        let s = sm.subscription("track");
        eventually(|| !s.needs_subscribe()).await; // the subscription that is kept should own the down track
        let st = fakes::downcast::<FakeSubscribedTrack>(s.get_subscribed_track().unwrap());
        eventually(|| {
            let n = st.update_subscriber_settings_call_count();
            if n == 0 {
                return false;
            }
            let (applied, _) = st.update_subscriber_settings_args_for_call(n - 1);
            applied == settings
        })
        .await; // the down track should get the settings

        sm.close(false);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_settings_kept_for_subscribe() {
    {
        // t.Run("media")
        let sm = new_test_subscription_manager();
        let resolver = new_test_resolver(true, true, "pub", "pubID");
        sm.set_track_resolver({
            let resolver = resolver.clone();
            move |sub, track_id| resolver.resolve(sub, track_id)
        });

        let settings = livekit::UpdateTrackSettings {
            disabled: true,
            ..Default::default()
        };
        sm.update_subscribed_track_settings(&"track".into(), &settings);
        // a cleanup pass between the settings and the subscribe must not drop the settings
        sm.reconcile_subscriptions();
        sm.subscribe_to_track(&"track".into(), false);

        let s = sm.subscription("track");
        eventually(|| !s.needs_subscribe()).await; // track should be subscribed
        let st = fakes::downcast::<FakeSubscribedTrack>(s.get_subscribed_track().unwrap());
        eventually(|| {
            let n = st.update_subscriber_settings_call_count();
            if n == 0 {
                return false;
            }
            let (applied, _) = st.update_subscriber_settings_args_for_call(n - 1);
            applied == settings
        })
        .await; // the down track should get the settings

        // settings for a track that is never subscribed are cleaned up after notFoundTimeout
        sm.update_subscribed_track_settings(&"other".into(), &settings);
        eventually(|| {
            let m = sm.lock.read();
            !m.subscriptions
                .contains_key(&livekit::TrackId::from("other"))
        })
        .await; // unused settings should be cleaned up
    }

    {
        let name = "data";
        let sm = new_test_subscription_manager();
        let resolver = new_test_data_track_resolver(true, true, "pub", "pubID");
        sm.set_data_track_resolver({
            let resolver = resolver.clone();
            move |sub, track_id| resolver.resolve(sub, track_id)
        });

        let fps = 5u32;
        let options = livekit::DataTrackSubscriptionOptions {
            target_fps: Some(fps),
        };
        sm.update_data_track_subscription_options(&"track".into(), Some(&options));
        // a cleanup pass between the options and the subscribe must not drop the options
        sm.reconcile_data_track_subscriptions();
        sm.subscribe_to_data_track(&"track".into());

        let s = sm.data_track_subscription("track");
        eventually(|| s.get_data_down_track().is_some()).await; // data track should be subscribed
        let ddt = fakes::downcast::<FakeDataDownTrack>(s.get_data_down_track().unwrap());
        let n = ddt.update_subscription_options_call_count();
        assert_ne!(0, n, "{name}");
        assert_eq!(
            Some(options),
            ddt.update_subscription_options_args_for_call(n - 1),
            "{name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_burst_overflows_queue() {
    // a dropped reconcile must not wait for the ticker
    // (the original sets `reconcileInterval` for the package while this test runs; here the
    // managers of this test are made with it)
    let reconcile_interval = HOUR;

    {
        // t.Run("media")
        let sm = new_test_subscription_manager_with_params(TestSubscriptionParams {
            reconcile_interval: Some(reconcile_interval),
            ..Default::default()
        });

        // the first subscribe holds the worker until all are queued, so the queue overflows
        let (release, released) = std::sync::mpsc::channel::<()>();
        let released = Arc::new(Mutex::new(released));
        let hold_once = Arc::new(std::sync::Once::new());
        sm.set_track_resolver(move |_, track_id| {
            let mt = Arc::new(FakeMediaTrack::default());
            mt.id_returns(track_id.clone());
            mt.add_subscriber_calls({
                // the fake keeps its stubs, and a stub that held the fake would keep it alive
                let mt = Arc::downgrade(&mt);
                let track_id = track_id.clone();
                let released = released.clone();
                let hold_once = hold_once.clone();
                move |_| {
                    // <-release: the channel is closed when its sending half is dropped
                    hold_once.call_once(|| {
                        let _ = released.lock().recv();
                    });
                    let st = Arc::new(FakeSubscribedTrack::default());
                    st.id_returns(track_id.clone());
                    st.media_track_returns(mt.upgrade().map(|mt| mt as Arc<dyn MediaTrack>));
                    Ok(Some(st as Arc<dyn SubscribedTrack>))
                }
            });
            MediaResolverResult {
                track: Some(mt),
                has_permission: true,
                publisher_id: "pubID".into(),
                publisher_identity: "pub".into(),
                track_changed_notifier: Some(crate::utils::ChangeNotifier::new()),
                track_removed_notifier: Some(crate::utils::ChangeNotifier::new()),
            }
        });

        let num_tracks = sm.reconcile_ch.max_capacity() + 10;
        for i in 0..num_tracks {
            sm.subscribe_to_track(&livekit::TrackId::from(format!("TR_{i}")), false);
        }
        drop(release);

        eventually(|| {
            let m = sm.lock.read();
            for s in m.subscriptions.values() {
                if s.needs_subscribe() {
                    return false;
                }
            }
            m.subscriptions.len() == num_tracks
        })
        .await; // every track should be subscribed
    }

    {
        // t.Run("data")
        let sm = new_test_subscription_manager_with_params(TestSubscriptionParams {
            reconcile_interval: Some(reconcile_interval),
            ..Default::default()
        });
        let resolver = new_test_data_track_resolver(true, true, "pub", "pubID");
        sm.set_data_track_resolver({
            let resolver = resolver.clone();
            move |sub, track_id| resolver.resolve(sub, track_id)
        });

        let (release, released) = std::sync::mpsc::channel::<()>();
        let released = Mutex::new(released);
        let hold_once = std::sync::Once::new();
        resolver.data_track.add_subscriber_calls({
            // the fake keeps its stubs, and a stub that held the fake would keep it alive
            let data_track = Arc::downgrade(&resolver.data_track);
            move |_| {
                hold_once.call_once(|| {
                    let _ = released.lock().recv();
                });
                let ddt = Arc::new(FakeDataDownTrack::default());
                ddt.publish_data_track_returns(
                    data_track.upgrade().map(|dt| dt as Arc<dyn DataTrack>),
                );
                Ok(Some(ddt as Arc<dyn DataDownTrack>))
            }
        });

        let num_tracks = sm.reconcile_data_track_ch.max_capacity() + 10;
        for i in 0..num_tracks {
            sm.subscribe_to_data_track(&livekit::TrackId::from(format!("DTR_{i}")));
        }
        drop(release);

        eventually(|| {
            let m = sm.lock.read();
            for s in m.data_track_subscriptions.values() {
                if s.needs_subscribe() {
                    return false;
                }
            }
            m.data_track_subscriptions.len() == num_tracks
        })
        .await; // every data track should be subscribed
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_limits() {
    let sm = new_test_subscription_manager_with_params(TestSubscriptionParams {
        subscription_limit_audio: 1,
        subscription_limit_video: 1,
        ..Default::default()
    });
    let resolver = new_test_resolver(true, true, "pub", "pubID");
    sm.set_track_resolver({
        let resolver = resolver.clone();
        move |sub, track_id| resolver.resolve(sub, track_id)
    });
    let sub_count = Arc::new(atomic::Int32::default());
    let failed = Arc::new(atomic::Bool::default());
    sm.set_on_track_subscribed({
        let sub_count = sub_count.clone();
        move |_sub_track| {
            sub_count.add(1);
        }
    });
    sm.set_on_subscription_error({
        let failed = failed.clone();
        move |_track_id, _fatal, _err| {
            failed.store(true);
        }
    });
    let num_participant_subscribed = Arc::new(atomic::Int32::default());
    let num_participant_unsubscribed = Arc::new(atomic::Int32::default());
    sm.on_subscribe_status_changed(Box::new({
        let num_participant_subscribed = num_participant_subscribed.clone();
        let num_participant_unsubscribed = num_participant_unsubscribed.clone();
        move |_pub_id, subscribed| {
            if subscribed {
                num_participant_subscribed.add(1);
            } else {
                num_participant_unsubscribed.add(1);
            }
        }
    }));

    sm.subscribe_to_track(&"track".into(), false);
    let s = sm.subscription("track");
    assert!(s.is_desired());
    eventually(|| sub_count.load() == 1).await; // track was not subscribed

    assert!(s.get_subscribed_track().is_some());
    assert_eq!(1, sm.get_subscribed_tracks().len());

    eventually(|| sm.get_subscribed_participants().len() == 1).await; // GetSubscribedParticipants should have returned one item
    assert_eq!("pubID", sm.get_subscribed_participants()[0].as_str());

    // ensure telemetry events are sent
    let tl = sm.telemetry_listener();
    assert_eq!(1, tl.on_track_subscribe_requested_call_count());

    // ensure bound
    set_test_subscribed_track_bound(s.get_subscribed_track().unwrap());
    eventually(|| !s.needs_bind()).await; // track was not bound

    // telemetry event should have been sent
    assert_eq!(1, tl.on_track_subscribed_call_count());

    // reach subscription limit, subscribe pending
    sm.subscribe_to_track(&"track2".into(), false);
    let s2 = sm.subscription("track2");
    time::sleep(SUBSCRIPTION_TIMEOUT * 2).await;
    assert!(s2.needs_subscribe());
    assert_eq!(2, tl.on_track_subscribe_requested_call_count());
    assert_eq!(1, tl.on_track_subscribe_failed_call_count());
    assert_eq!(1, sm.get_subscribed_tracks().len());

    // unsubscribe track1, then track2 should be subscribed
    sm.unsubscribe_from_track(&"track".into());
    assert!(!s.is_desired());
    assert!(s.needs_unsubscribe());
    // wait for unsubscribe to take effect
    time::sleep(RECONCILE_INTERVAL).await;
    set_test_subscribed_track_closed(s.get_subscribed_track().unwrap(), false);
    assert!(s.get_subscribed_track().is_none());

    time::sleep(RECONCILE_INTERVAL).await;
    assert!(s2.is_desired());
    assert!(!s2.needs_subscribe());
    assert_eq!(2, sub_count.load());
    assert!(s2.get_subscribed_track().is_some());
    assert_eq!(2, tl.on_track_subscribe_requested_call_count());
    assert_eq!(1, sm.get_subscribed_tracks().len());

    // ensure bound
    set_test_subscribed_track_bound(s2.get_subscribed_track().unwrap());
    eventually(|| !s2.needs_bind()).await; // track was not bound

    // subscribe to track1 again, which should pending
    sm.subscribe_to_track(&"track".into(), false);
    let s = sm.subscription("track");
    assert!(s.is_desired());
    time::sleep(SUBSCRIPTION_TIMEOUT * 2).await;
    assert!(s.needs_subscribe());
    assert_eq!(3, tl.on_track_subscribe_requested_call_count());
    assert_eq!(2, tl.on_track_subscribe_failed_call_count());
    assert_eq!(1, sm.get_subscribed_tracks().len());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_data_track() {
    {
        let name = "no track permission";
        let sm = new_test_subscription_manager();
        let resolver = new_test_data_track_resolver(false, true, "pub", "pubID");
        sm.set_data_track_resolver({
            let resolver = resolver.clone();
            move |sub, track_id| resolver.resolve(sub, track_id)
        });

        sm.subscribe_to_data_track(&"track".into());
        let s = sm.data_track_subscription("track");
        eventually(|| !s.get_has_permission()).await; // should not have permission to subscribe

        time::sleep(SUBSCRIPTION_TIMEOUT).await;

        // isDesired remains unchanged, no subscriber added to the data track
        assert!(s.is_desired(), "{name}");
        assert!(s.needs_subscribe(), "{name}");
        assert_eq!(0, resolver.data_track.add_subscriber_call_count(), "{name}");

        // subscriber should have been notified that permission was denied
        let p = &sm.participant;
        assert_eq!(
            1,
            p.send_subscription_permission_update_call_count(),
            "{name}"
        );
        let (_, _, allowed) = p.send_subscription_permission_update_args_for_call(0);
        assert!(!allowed, "{name}");

        // give permissions now
        resolver.set_has_permission(true);

        eventually(|| !s.needs_subscribe()).await; // should be subscribed
        assert!(s.get_data_down_track().is_some(), "{name}");
        assert_eq!(1, resolver.data_track.add_subscriber_call_count(), "{name}");
    }

    {
        let name = "permission revoked while subscribed";
        let sm = new_test_subscription_manager();
        let resolver = new_test_data_track_resolver(true, true, "pub", "pubID");
        sm.set_data_track_resolver({
            let resolver = resolver.clone();
            move |sub, track_id| resolver.resolve(sub, track_id)
        });

        sm.subscribe_to_data_track(&"track".into());
        let s = sm.data_track_subscription("track");
        eventually(|| !s.needs_subscribe()).await; // should be subscribed

        // revoke permission and close the down track like the publisher would
        // (DataTrack.RevokeDisallowedSubscribers -> RemoveSubscriber -> DataDownTrack.Close)
        resolver.set_has_permission(false);
        let ddt = s.get_data_down_track();
        assert!(ddt.is_some(), "{name}");
        set_test_data_down_track_closed(ddt.unwrap());

        // subscription stays desired but is denied on resubscribe attempts
        assert!(s.get_data_down_track().is_none(), "{name}");
        assert!(s.is_desired(), "{name}");
        eventually(|| !s.get_has_permission()).await; // should not have permission to resubscribe
        assert!(s.needs_subscribe(), "{name}");
        assert_eq!(1, resolver.data_track.add_subscriber_call_count(), "{name}");

        // give permission back, should resubscribe
        resolver.set_has_permission(true);

        eventually(|| !s.needs_subscribe() && s.get_data_down_track().is_some()).await; // should be resubscribed
        assert_eq!(2, resolver.data_track.add_subscriber_call_count(), "{name}");
    }

    {
        let name = "subscribe again during unsubscribe";
        let sm = new_test_subscription_manager();
        let resolver = new_test_data_track_resolver(true, true, "pub", "pubID");
        sm.set_data_track_resolver({
            let resolver = resolver.clone();
            move |sub, track_id| resolver.resolve(sub, track_id)
        });

        sm.subscribe_to_data_track(&"track".into());
        let s = sm.data_track_subscription("track");
        eventually(|| !s.needs_subscribe()).await; // should be subscribed

        // the client subscribes again while the removal runs,
        // then the down track closes, as DataTrack.RemoveSubscriber does
        let ddt = fakes::downcast::<FakeDataDownTrack>(s.get_data_down_track().unwrap());
        resolver.data_track.remove_subscriber_calls({
            let sm = sm.sm.clone();
            move |_| {
                sm.subscribe_to_data_track(&"track".into());
                data_down_track_on_close_args_for_call(&ddt, 0).unwrap()();
            }
        });
        sm.unsubscribe_from_data_track(&"track".into());

        eventually(|| {
            resolver.data_track.add_subscriber_call_count() == 2
                && s.get_data_down_track().is_some()
        })
        .await; // should be subscribed again
        assert!(
            Arc::ptr_eq(&s, &sm.data_track_subscription("track")),
            "{name}"
        );
    }

    {
        // t.Run("unsubscribe before data track resolves")
        let sm = new_test_subscription_manager();
        // no track available, subscribe attempts fail with ErrTrackNotFound
        let resolver = new_test_data_track_resolver(true, false, "pub", "pubID");
        sm.set_data_track_resolver({
            let resolver = resolver.clone();
            move |sub, track_id| resolver.resolve(sub, track_id)
        });

        sm.subscribe_to_data_track(&"track".into());
        // (the original asserts here that the subscription is there; this one panics if not)
        let s = sm.data_track_subscription("track");

        // let the worker attempt (and fail) the subscribe
        eventually(|| s.get_num_attempts() > 0).await; // no subscribe attempt was made

        sm.unsubscribe_from_data_track(&"track".into());
        eventually(|| {
            let m = sm.lock.read();
            !m.data_track_subscriptions
                .contains_key(&livekit::TrackId::from("track"))
        })
        .await; // data track subscription was not cleaned up
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_pacer_takes_video_tokens_only() {
    struct Tc {
        name: &'static str,
        kind: livekit::TrackType,
        is_sync: bool,
        setup_fails: bool,
        token_is_used: bool,
    }
    for tc in [
        Tc {
            name: "video",
            kind: livekit::TrackType::Video,
            is_sync: false,
            setup_fails: false,
            token_is_used: true,
        },
        Tc {
            name: "audio",
            kind: livekit::TrackType::Audio,
            is_sync: false,
            setup_fails: false,
            token_is_used: false,
        },
        Tc {
            name: "video one-shot",
            kind: livekit::TrackType::Video,
            is_sync: true,
            setup_fails: false,
            token_is_used: false,
        },
        // a failed setup spends its token, like a real pacer that gates before setup
        Tc {
            name: "video setup fails",
            kind: livekit::TrackType::Video,
            is_sync: false,
            setup_fails: true,
            token_is_used: true,
        },
    ] {
        // a rate this low holds one token and takes 1000 s to refill it, so after one
        // take the next subscribe waits however slow the test runs
        let pacer = SubscribePacer::new(&[0.001]).unwrap();
        let sm = new_test_subscription_manager_with_params(TestSubscriptionParams {
            subscribe_pacer: Some(pacer.clone()),
            ..Default::default()
        });
        let attempted = Arc::new(atomic::Bool::default());
        let resolver = new_test_resolver(true, true, "pub", "pubID");
        sm.set_track_resolver({
            let attempted = attempted.clone();
            let (kind, setup_fails) = (tc.kind, tc.setup_fails);
            move |sub, track_id| {
                let res = resolver.resolve(sub, track_id);
                let mt = fakes::downcast::<FakeMediaTrack>(res.track.clone().unwrap());
                mt.kind_returns(kind);
                if setup_fails {
                    let attempted = attempted.clone();
                    mt.add_subscriber_calls(move |_| {
                        attempted.store(true);
                        Err(Error::NoReceiver)
                    });
                }
                res
            }
        });
        sm.set_on_track_subscribed({
            let attempted = attempted.clone();
            move |_sub_track| {
                attempted.store(true);
            }
        });

        sm.subscribe_to_track(&"track".into(), tc.is_sync);
        eventually(|| attempted.load()).await; // subscribe was not attempted

        assert_eq!(
            tc.token_is_used,
            pacer.delays(time::now())["0.001"] > Duration::ZERO,
            "{}",
            tc.name
        );
    }
}

// A canSubscribe revoke that lands while a subscribe is in flight cannot see the subscription
// yet. The participant here allows the subscribe at its start and is revoked by the time the
// subscription is recorded, so the subscribe must undo itself.
#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_revoked_in_flight() {
    let revoke_after_first_check = |sm: &TestSubscriptionManager| {
        let p = &sm.participant;
        p.can_subscribe_returns_on_call(0, true);
        p.can_subscribe_returns(false);
    };

    {
        let name = "media";
        let sm = new_test_subscription_manager();
        revoke_after_first_check(&sm);

        let resolver = new_test_resolver(true, true, "pub", "pubID");
        let tracks: Arc<Mutex<Vec<Arc<FakeMediaTrack>>>> = Arc::default();
        sm.set_track_resolver({
            let tracks = tracks.clone();
            move |sub, track_id| {
                let res = resolver.resolve(sub, track_id);
                tracks.lock().push(fakes::downcast::<FakeMediaTrack>(
                    res.track.clone().unwrap(),
                ));
                res
            }
        });
        let subscribed = Arc::new(atomic::Bool::default());
        sm.set_on_track_subscribed({
            let subscribed = subscribed.clone();
            move |_sub_track| {
                subscribed.store(true);
            }
        });

        sm.subscribe_to_track(&"track".into(), false);
        eventually(|| {
            let tracks = tracks.lock();
            !tracks.is_empty() && tracks[0].remove_subscriber_call_count() == 1
        })
        .await; // the subscriber should be removed

        let (sub_id, is_expected_to_resume) = tracks.lock()[0].remove_subscriber_args_for_call(0);
        assert_eq!(sm.participant.id(), sub_id, "{name}");
        assert!(!is_expected_to_resume, "{name}");
        assert!(sm.get_subscribed_participants().is_empty(), "{name}");
        assert!(!subscribed.load(), "{name}");
    }

    {
        let name = "data";
        let sm = new_test_subscription_manager();
        revoke_after_first_check(&sm);

        let resolver = new_test_data_track_resolver(true, true, "pub", "pubID");
        sm.set_data_track_resolver({
            let resolver = resolver.clone();
            move |sub, track_id| resolver.resolve(sub, track_id)
        });

        sm.subscribe_to_data_track(&"track".into());
        eventually(|| resolver.data_track.remove_subscriber_call_count() == 1).await; // the subscriber should be removed

        assert_eq!(
            sm.participant.id(),
            resolver.data_track.remove_subscriber_args_for_call(0),
            "{name}"
        );
        assert!(sm.get_subscribed_participants().is_empty(), "{name}");
    }
}

// require.Eventually(t, condition, subSettleTimeout, subCheckInterval, msg)
#[track_caller]
fn eventually(condition: impl FnMut() -> bool) -> impl Future<Output = ()> {
    let wait = eventually_async(condition, SUB_SETTLE_TIMEOUT, SUB_CHECK_INTERVAL);
    async move {
        // testify's first look is after one tick
        tokio::time::sleep(SUB_CHECK_INTERVAL).await;
        wait.await
    }
}

#[derive(Default)]
struct TestSubscriptionParams {
    subscription_limit_audio: i32,
    subscription_limit_video: i32,

    // not in the original, which sets them after the manager is made: on its params and
    // for the package
    subscribe_pacer: Option<Arc<SubscribePacer>>,
    reconcile_interval: Option<Duration>,
}

type TrackResolverFn =
    Arc<dyn Fn(Arc<dyn LocalParticipant>, &livekit::TrackId) -> MediaResolverResult + Send + Sync>;
type DataTrackResolverFn =
    Arc<dyn Fn(Arc<dyn LocalParticipant>, &livekit::TrackId) -> DataResolverResult + Send + Sync>;
type OnTrackFn = Arc<dyn Fn(Arc<dyn SubscribedTrack>) + Send + Sync>;
type OnSubscriptionErrorFn = Arc<dyn Fn(&livekit::TrackId, bool, &Error) + Send + Sync>;

// The fields of `sm.params` that the tests replace after the manager is made: the params of
// a test manager pass each call on to what is here.
struct TestParams {
    track_resolver: Mutex<TrackResolverFn>,
    on_track_subscribed: Mutex<OnTrackFn>,
    on_track_unsubscribed: Mutex<OnTrackFn>,
    on_subscription_error: Mutex<OnSubscriptionErrorFn>,
    data_track_resolver: Mutex<DataTrackResolverFn>,
}

// The manager of a test, with what the original's tests reach through `sm.params`.
struct TestSubscriptionManager {
    sm: Arc<SubscriptionManager>,
    // sm.params.Participant.(*typesfakes.FakeLocalParticipant)
    participant: Arc<FakeLocalParticipant>,
    test_params: Arc<TestParams>,
}

impl Deref for TestSubscriptionManager {
    type Target = SubscriptionManager;

    fn deref(&self) -> &SubscriptionManager {
        &self.sm
    }
}

// defer sm.Close(false)
impl Drop for TestSubscriptionManager {
    fn drop(&mut self) {
        self.sm.close(false);
    }
}

impl TestSubscriptionManager {
    // sm.params.TrackResolver = f
    fn set_track_resolver(
        &self,
        f: impl Fn(Arc<dyn LocalParticipant>, &livekit::TrackId) -> MediaResolverResult
        + Send
        + Sync
        + 'static,
    ) {
        *self.test_params.track_resolver.lock() = Arc::new(f);
    }

    // sm.params.OnTrackSubscribed = f
    fn set_on_track_subscribed(
        &self,
        f: impl Fn(Arc<dyn SubscribedTrack>) + Send + Sync + 'static,
    ) {
        *self.test_params.on_track_subscribed.lock() = Arc::new(f);
    }

    // sm.params.OnTrackUnsubscribed = f
    fn set_on_track_unsubscribed(
        &self,
        f: impl Fn(Arc<dyn SubscribedTrack>) + Send + Sync + 'static,
    ) {
        *self.test_params.on_track_unsubscribed.lock() = Arc::new(f);
    }

    // sm.params.OnSubscriptionError = f
    fn set_on_subscription_error(
        &self,
        f: impl Fn(&livekit::TrackId, bool, &Error) + Send + Sync + 'static,
    ) {
        *self.test_params.on_subscription_error.lock() = Arc::new(f);
    }

    // sm.params.DataTrackResolver = f
    fn set_data_track_resolver(
        &self,
        f: impl Fn(Arc<dyn LocalParticipant>, &livekit::TrackId) -> DataResolverResult
        + Send
        + Sync
        + 'static,
    ) {
        *self.test_params.data_track_resolver.lock() = Arc::new(f);
    }

    // sm.params.Participant.GetTelemetryListener().(*typesfakes.FakeParticipantTelemetryListener)
    fn telemetry_listener(&self) -> Arc<FakeParticipantTelemetryListener> {
        fakes::downcast::<FakeParticipantTelemetryListener>(
            self.participant.get_telemetry_listener(),
        )
    }

    // sm.subscriptions[trackID]
    fn subscription(&self, track_id: &str) -> Arc<MediaTrackSubscription> {
        self.sm
            .lock
            .read()
            .subscriptions
            .get(&livekit::TrackId::from(track_id))
            .cloned()
            .unwrap_or_else(|| panic!("no subscription for {track_id}"))
    }

    // sm.dataTrackSubscriptions[trackID]
    fn data_track_subscription(&self, track_id: &str) -> Arc<DataTrackSubscription> {
        self.sm
            .lock
            .read()
            .data_track_subscriptions
            .get(&livekit::TrackId::from(track_id))
            .cloned()
            .unwrap_or_else(|| panic!("no data track subscription for {track_id}"))
    }
}

fn new_test_subscription_manager() -> TestSubscriptionManager {
    new_test_subscription_manager_with_params(TestSubscriptionParams::default())
}

fn new_test_subscription_manager_with_params(
    params: TestSubscriptionParams,
) -> TestSubscriptionManager {
    let p = Arc::new(FakeLocalParticipant::default());
    p.can_subscribe_returns(true);
    p.id_returns("subID".into());
    p.identity_returns("sub".into());
    p.kind_returns(livekit::participant_info::Kind::Standard);

    let tl = Arc::new(FakeParticipantTelemetryListener::default());
    p.get_telemetry_listener_returns(tl);

    let test_params = Arc::new(TestParams {
        track_resolver: Mutex::new(Arc::new(
            |_sub: Arc<dyn LocalParticipant>, _track_id: &livekit::TrackId| {
                MediaResolverResult::default()
            },
        )),
        on_track_subscribed: Mutex::new(Arc::new(|_sub_track: Arc<dyn SubscribedTrack>| {})),
        on_track_unsubscribed: Mutex::new(Arc::new(|_sub_track: Arc<dyn SubscribedTrack>| {})),
        on_subscription_error: Mutex::new(Arc::new(
            |_track_id: &livekit::TrackId, _fatal: bool, _err: &Error| {},
        )),
        // nil in the original
        data_track_resolver: Mutex::new(Arc::new(
            |_sub: Arc<dyn LocalParticipant>, _track_id: &livekit::TrackId| {
                DataResolverResult::default()
            },
        )),
    });

    let participant: Arc<dyn LocalParticipant> = p.clone();
    let sm = SubscriptionManager::new_with_reconcile_interval(
        SubscriptionManagerParams {
            participant: Arc::downgrade(&participant),
            logger: logger::get_logger(),
            on_track_subscribed: Box::new({
                let test_params = test_params.clone();
                move |sub_track| {
                    let f = test_params.on_track_subscribed.lock().clone();
                    f(sub_track)
                }
            }),
            on_track_unsubscribed: Box::new({
                let test_params = test_params.clone();
                move |sub_track| {
                    let f = test_params.on_track_unsubscribed.lock().clone();
                    f(sub_track)
                }
            }),
            on_subscription_error: Box::new({
                let test_params = test_params.clone();
                move |track_id, fatal, err| {
                    let f = test_params.on_subscription_error.lock().clone();
                    f(track_id, fatal, err)
                }
            }),
            track_resolver: Box::new({
                let test_params = test_params.clone();
                move |sub, track_id| {
                    let f = test_params.track_resolver.lock().clone();
                    f(sub, track_id)
                }
            }),
            subscription_limit_audio: params.subscription_limit_audio,
            subscription_limit_video: params.subscription_limit_video,
            subscribe_pacer: params.subscribe_pacer,
            data_track_resolver: Box::new({
                let test_params = test_params.clone();
                move |sub, track_id| {
                    let f = test_params.data_track_resolver.lock().clone();
                    f(sub, track_id)
                }
            }),
            use_one_shot_signalling_mode: false,
        },
        params.reconcile_interval.unwrap_or(RECONCILE_INTERVAL),
    );

    TestSubscriptionManager {
        sm,
        participant: p,
        test_params,
    }
}

struct TestResolver {
    lock: Mutex<TestResolverInner>,
    pub_identity: livekit::ParticipantIdentity,
    pub_id: livekit::ParticipantId,
}

struct TestResolverInner {
    has_permission: bool,
    has_track: bool,

    paused: bool,
}

fn new_test_resolver(
    has_permission: bool,
    has_track: bool,
    pub_identity: &str,
    pub_id: &str,
) -> Arc<TestResolver> {
    Arc::new(TestResolver {
        lock: Mutex::new(TestResolverInner {
            has_permission,
            has_track,
            paused: false,
        }),
        pub_identity: pub_identity.into(),
        pub_id: pub_id.into(),
    })
}

impl TestResolver {
    fn set_pause(&self, paused: bool) {
        let mut t = self.lock.lock();
        t.paused = paused;
    }

    fn resolve(
        &self,
        _subscriber: Arc<dyn LocalParticipant>,
        track_id: &livekit::TrackId,
    ) -> MediaResolverResult {
        let t = self.lock.lock();
        let mut res = MediaResolverResult {
            track_changed_notifier: Some(crate::utils::ChangeNotifier::new()),
            track_removed_notifier: Some(crate::utils::ChangeNotifier::new()),
            has_permission: t.has_permission,
            publisher_id: self.pub_id.clone(),
            publisher_identity: self.pub_identity.clone(),
            ..Default::default()
        };
        if t.has_track && !t.paused {
            let mt = Arc::new(FakeMediaTrack::default());
            let st = new_fake_subscribed_track();
            st.id_returns(track_id.clone());
            st.publisher_id_returns(self.pub_id.clone());
            st.publisher_identity_returns(self.pub_identity.clone());
            mt.add_subscriber_calls({
                let st = st.clone();
                move |sub| {
                    st.subscriber_returns(sub);
                    Ok(Some(st.clone() as Arc<dyn SubscribedTrack>))
                }
            });
            st.media_track_returns(Some(mt.clone()));
            res.track = Some(mt);
        }
        res
    }
}

struct TestDataTrackResolver {
    lock: Mutex<TestDataTrackResolverInner>,
    pub_identity: livekit::ParticipantIdentity,
    pub_id: livekit::ParticipantId,

    data_track: Arc<FakeDataTrack>,
}

struct TestDataTrackResolverInner {
    has_permission: bool,
    has_track: bool,
}

fn new_test_data_track_resolver(
    has_permission: bool,
    has_track: bool,
    pub_identity: &str,
    pub_id: &str,
) -> Arc<TestDataTrackResolver> {
    let r = Arc::new(TestDataTrackResolver {
        lock: Mutex::new(TestDataTrackResolverInner {
            has_permission,
            has_track,
        }),
        pub_identity: pub_identity.into(),
        pub_id: pub_id.into(),
        data_track: Arc::new(FakeDataTrack::default()),
    });
    r.data_track.publisher_id_returns(pub_id.into());
    r.data_track.publisher_identity_returns(pub_identity.into());
    r.data_track.add_subscriber_calls({
        // the fake keeps its stubs, and a stub that held the fake would keep it alive
        let data_track = Arc::downgrade(&r.data_track);
        move |_sub| {
            let ddt = new_fake_data_down_track();
            ddt.publish_data_track_returns(data_track.upgrade().map(|dt| dt as Arc<dyn DataTrack>));
            Ok(Some(ddt as Arc<dyn DataDownTrack>))
        }
    });
    r
}

impl TestDataTrackResolver {
    fn set_has_permission(&self, has_permission: bool) {
        let mut t = self.lock.lock();
        t.has_permission = has_permission;
    }

    fn resolve(
        &self,
        _subscriber: Arc<dyn LocalParticipant>,
        track_id: &livekit::TrackId,
    ) -> DataResolverResult {
        let t = self.lock.lock();
        let mut res = DataResolverResult {
            track_changed_notifier: Some(crate::utils::ChangeNotifier::new()),
            track_removed_notifier: Some(crate::utils::ChangeNotifier::new()),
            has_permission: t.has_permission,
            publisher_id: self.pub_id.clone(),
            publisher_identity: self.pub_identity.clone(),
            ..Default::default()
        };
        if t.has_track {
            self.data_track.id_returns(track_id.clone());
            res.data_track = Some(self.data_track.clone());
        }
        res
    }
}

fn set_test_subscribed_track_bound(st: Arc<dyn SubscribedTrack>) {
    let fst = fakes::downcast::<FakeSubscribedTrack>(st);

    for i in 0..fst.add_on_bind_call_count() {
        subscribed_track_add_on_bind_args_for_call(&fst, i)(Ok(()));
    }
}

fn set_test_subscribed_track_closed(st: Arc<dyn SubscribedTrack>, is_expected_to_resume: bool) {
    let fst = fakes::downcast::<FakeSubscribedTrack>(st);

    subscribed_track_on_close_args_for_call(&fst, 0).unwrap()(is_expected_to_resume);
}

fn set_test_data_down_track_closed(ddt: Arc<dyn DataDownTrack>) {
    let fddt = fakes::downcast::<FakeDataDownTrack>(ddt);

    data_down_track_on_close_args_for_call(&fddt, 0).unwrap()();
}

// ------------------------------------------------
// The callbacks that a fake was given. New: counterfeiter's fakes record them, the fakes
// here do not (see the header). A table has an entry per fake, under its address; the
// entry holds the fake weakly, which keeps the address from being given to another fake.

type OnClose = Arc<dyn Fn(bool) + Send + Sync>;
type OnBind = Arc<dyn Fn(Result<(), Error>) + Send + Sync>;
type OnDataDownTrackClose = Arc<dyn Fn() + Send + Sync>;

#[derive(Default)]
struct SubscribedTrackCallbacks {
    on_close: Vec<Option<OnClose>>,
    add_on_bind: Vec<OnBind>,
}

type CallbackTable<F, C> = LazyLock<Mutex<HashMap<usize, (Weak<F>, C)>>>;

static SUBSCRIBED_TRACK_CALLBACKS: CallbackTable<FakeSubscribedTrack, SubscribedTrackCallbacks> =
    LazyLock::new(Default::default);
static DATA_DOWN_TRACK_CALLBACKS: CallbackTable<
    FakeDataDownTrack,
    Vec<Option<OnDataDownTrackClose>>,
> = LazyLock::new(Default::default);

// &typesfakes.FakeSubscribedTrack{}, for a test that calls what the track was given
fn new_fake_subscribed_track() -> Arc<FakeSubscribedTrack> {
    let st = Arc::new(FakeSubscribedTrack::default());
    let key = Arc::as_ptr(&st) as usize;
    {
        let mut callbacks = SUBSCRIBED_TRACK_CALLBACKS.lock();
        // the entry of a fake that is gone is of no use to anybody
        callbacks.retain(|_, (fake, _)| fake.strong_count() != 0);
        callbacks.insert(key, (Arc::downgrade(&st), Default::default()));
    }
    st.on_close_calls(move |f| {
        if let Some((_, callbacks)) = SUBSCRIBED_TRACK_CALLBACKS.lock().get_mut(&key) {
            callbacks.on_close.push(f.map(Arc::from));
        }
    });
    st.add_on_bind_calls(move |f| {
        if let Some((_, callbacks)) = SUBSCRIBED_TRACK_CALLBACKS.lock().get_mut(&key) {
            callbacks.add_on_bind.push(Arc::from(f));
        }
    });
    st
}

// &typesfakes.FakeDataDownTrack{}, for a test that calls what the track was given
fn new_fake_data_down_track() -> Arc<FakeDataDownTrack> {
    let ddt = Arc::new(FakeDataDownTrack::default());
    let key = Arc::as_ptr(&ddt) as usize;
    {
        let mut callbacks = DATA_DOWN_TRACK_CALLBACKS.lock();
        callbacks.retain(|_, (fake, _)| fake.strong_count() != 0);
        callbacks.insert(key, (Arc::downgrade(&ddt), Default::default()));
    }
    ddt.on_close_calls(move |f| {
        if let Some((_, on_close)) = DATA_DOWN_TRACK_CALLBACKS.lock().get_mut(&key) {
            on_close.push(f.map(Arc::from));
        }
    });
    ddt
}

// fst.OnCloseArgsForCall(i)
fn subscribed_track_on_close_args_for_call(
    fst: &Arc<FakeSubscribedTrack>,
    i: usize,
) -> Option<OnClose> {
    let callbacks = SUBSCRIBED_TRACK_CALLBACKS.lock();
    let (fake, callbacks) = &callbacks[&(Arc::as_ptr(fst) as usize)];
    assert!(Weak::ptr_eq(fake, &Arc::downgrade(fst)));
    callbacks.on_close[i].clone()
}

// fst.AddOnBindArgsForCall(i)
fn subscribed_track_add_on_bind_args_for_call(fst: &Arc<FakeSubscribedTrack>, i: usize) -> OnBind {
    let callbacks = SUBSCRIBED_TRACK_CALLBACKS.lock();
    let (fake, callbacks) = &callbacks[&(Arc::as_ptr(fst) as usize)];
    assert!(Weak::ptr_eq(fake, &Arc::downgrade(fst)));
    callbacks.add_on_bind[i].clone()
}

// fddt.OnCloseArgsForCall(i)
fn data_down_track_on_close_args_for_call(
    fddt: &Arc<FakeDataDownTrack>,
    i: usize,
) -> Option<OnDataDownTrackClose> {
    let callbacks = DATA_DOWN_TRACK_CALLBACKS.lock();
    let (fake, on_close) = &callbacks[&(Arc::as_ptr(fddt) as usize)];
    assert!(Weak::ptr_eq(fake, &Arc::downgrade(fddt)));
    on_close[i].clone()
}
