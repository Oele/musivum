// New for musivum: the original has no tests of the room track manager.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;

use crate::protocol::logger;
use crate::rtc::types::{FakeDataTrack, FakeMediaTrack};
use crate::testutils::eventually_async;

fn new_track(id: &str) -> Arc<FakeMediaTrack> {
    let t = Arc::new(FakeMediaTrack::default());
    t.id_returns(id.into());
    t.is_open_returns(true);
    t
}

fn new_data_track(id: &str) -> Arc<FakeDataTrack> {
    let t = Arc::new(FakeDataTrack::default());
    t.id_returns(id.into());
    t
}

fn is_track(info: &Option<TrackInfo>, track: &Arc<FakeMediaTrack>) -> bool {
    let track: Arc<dyn types::MediaTrack> = track.clone();
    info.as_ref()
        .is_some_and(|info| Arc::ptr_eq(&info.track, &track))
}

#[test]
fn a_track_is_found_by_its_id_with_its_publisher_until_it_is_removed() {
    let r = RoomTrackManager::new(logger::get_logger());
    let id = livekit::TrackId::from("TR_a");

    assert!(r.get_track_info(&id).is_none());
    assert_eq!((0, 0), r.report());

    let of_alice = new_track("TR_a");
    r.add_track(of_alice.clone(), &"alice".into(), &"PA_alice".into());
    let info = r.get_track_info(&id);
    assert!(is_track(&info, &of_alice));
    let info = info.unwrap();
    assert_eq!("alice", info.publisher_identity.as_str());
    assert_eq!("PA_alice", info.publisher_id.as_str());
    assert!(r.get_track_info(&"TR_b".into()).is_none());
    assert_eq!((1, 0), r.report());

    // the same track is not added twice
    r.add_track(of_alice.clone(), &"alice".into(), &"PA_alice".into());
    assert_eq!(1, r.lock.read().tracks[&id].len());

    // another track under the same ID waits behind the first: the earliest is the one in use
    let of_bob = new_track("TR_a");
    r.add_track(of_bob.clone(), &"bob".into(), &"PA_bob".into());
    assert!(is_track(&r.get_track_info(&id), &of_alice));
    assert_eq!(2, r.lock.read().tracks[&id].len());
    assert_eq!((1, 0), r.report());

    // a track that is closing is not resolved, and the one behind it not yet either
    of_alice.is_open_returns(false);
    assert!(r.get_track_info(&id).is_none());
    of_alice.is_open_returns(true);

    // removing a track that was not added changes nothing
    r.remove_track(new_track("TR_a"));
    r.remove_track(new_track("TR_c"));
    assert!(is_track(&r.get_track_info(&id), &of_alice));
    assert_eq!(2, r.lock.read().tracks[&id].len());

    r.remove_track(of_alice.clone());
    let info = r.get_track_info(&id);
    assert!(is_track(&info, &of_bob));
    let info = info.unwrap();
    assert_eq!("bob", info.publisher_identity.as_str());
    assert_eq!("PA_bob", info.publisher_id.as_str());
    assert_eq!((1, 0), r.report());

    r.remove_track(of_bob.clone());
    assert!(r.get_track_info(&id).is_none());
    assert_eq!((0, 0), r.report());
    assert!(r.lock.read().tracks.is_empty());
}

#[test]
fn a_data_track_is_found_by_its_id_with_its_publisher_until_it_is_removed() {
    let r = RoomTrackManager::new(logger::get_logger());
    let id = livekit::TrackId::from("DTR_a");
    let is_data_track = |info: &Option<DataTrackInfo>, data_track: &Arc<FakeDataTrack>| {
        let data_track: Arc<dyn types::DataTrack> = data_track.clone();
        info.as_ref()
            .is_some_and(|info| Arc::ptr_eq(&info.data_track, &data_track))
    };

    assert!(r.get_data_track_info(&id).is_none());

    let of_alice = new_data_track("DTR_a");
    r.add_data_track(of_alice.clone(), &"alice".into(), &"PA_alice".into());
    r.add_data_track(of_alice.clone(), &"alice".into(), &"PA_alice".into());
    let of_bob = new_data_track("DTR_a");
    r.add_data_track(of_bob.clone(), &"bob".into(), &"PA_bob".into());
    assert_eq!(2, r.lock.read().data_tracks[&id].len());
    assert_eq!((0, 1), r.report());

    let info = r.get_data_track_info(&id);
    assert!(is_data_track(&info, &of_alice));
    let info = info.unwrap();
    assert_eq!("alice", info.publisher_identity.as_str());
    assert_eq!("PA_alice", info.publisher_id.as_str());
    // the media tracks are another map
    assert!(r.get_track_info(&id).is_none());

    r.remove_data_track(new_data_track("DTR_a"));
    assert_eq!(2, r.lock.read().data_tracks[&id].len());

    r.remove_data_track(of_alice.clone());
    let info = r.get_data_track_info(&id);
    assert!(is_data_track(&info, &of_bob));
    assert_eq!("PA_bob", info.unwrap().publisher_id.as_str());

    r.remove_data_track(of_bob.clone());
    assert!(r.get_data_track_info(&id).is_none());
    assert_eq!((0, 0), r.report());
}

#[tokio::test(flavor = "multi_thread")]
async fn who_waits_for_a_track_is_told_when_it_arrives_and_when_it_goes() {
    const WAIT: Duration = Duration::from_secs(5);
    const TICK: Duration = Duration::from_millis(5);

    let r = RoomTrackManager::new(logger::get_logger());
    let id = livekit::TrackId::from("TR_a");
    let track = new_track("TR_a");

    // nobody waits: a track that arrives makes no notifier, and has no observers
    r.add_track(track.clone(), &"alice".into(), &"PA_alice".into());
    assert!(r.changed_notifier.get_notifier("TR_a").is_none());
    assert!(r.removed_notifier.get_notifier("TR_a").is_none());
    assert!(!r.has_observers(track.clone()));
    r.remove_track(track.clone());

    // somebody waits for a track that is not there yet
    let changed = Arc::new(AtomicUsize::new(0));
    let c = changed.clone();
    r.get_or_create_track_change_notifier(&id)
        .add_observer("subscriber", move || {
            c.fetch_add(1, Ordering::SeqCst);
        });
    // it is not the track that is observed while it is not there
    assert!(!r.has_observers(track.clone()));

    r.add_track(track.clone(), &"alice".into(), &"PA_alice".into());
    eventually_async(|| changed.load(Ordering::SeqCst) == 1, WAIT, TICK).await;
    assert!(r.has_observers(track.clone()));
    // another track under the same ID is not the one in use, so not the one observed
    assert!(!r.has_observers(new_track("TR_a")));

    r.notify_track_changed(&id);
    eventually_async(|| changed.load(Ordering::SeqCst) == 2, WAIT, TICK).await;
    // a notifier is per track
    r.notify_track_changed(&"TR_b".into());

    let removed = Arc::new(AtomicUsize::new(0));
    let c = removed.clone();
    r.get_or_create_track_remove_notifier(&id)
        .add_observer("subscriber", move || {
            c.fetch_add(1, Ordering::SeqCst);
        });

    // a track that was not added takes no notifier with it
    r.remove_track(new_track("TR_a"));
    assert!(r.changed_notifier.get_notifier("TR_a").is_some());
    assert!(r.removed_notifier.get_notifier("TR_a").is_some());

    // the track goes: who waits for that is told, and both notifiers go with the track,
    // observers or not
    r.remove_track(track.clone());
    eventually_async(|| removed.load(Ordering::SeqCst) == 1, WAIT, TICK).await;
    assert!(r.changed_notifier.get_notifier("TR_a").is_none());
    assert!(r.removed_notifier.get_notifier("TR_a").is_none());
    assert!(!r.has_observers(track.clone()));
    assert_eq!(2, changed.load(Ordering::SeqCst));
}
