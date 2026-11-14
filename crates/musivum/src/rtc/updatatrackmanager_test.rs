// New for musivum: the original has no test of the manager. What is expected is the
// original's: it is what a Go test laid over `pkg/rtc` at v1.13.11 printed for the same
// calls.

use std::sync::Arc;

use super::*;
use crate::protocol::logger;
use crate::rtc::types::{FakeDataTrack, FakeParticipant, FakeParticipantListener};

fn new_data_track(pub_handle: u16, name: &str) -> Arc<FakeDataTrack> {
    let dt = Arc::new(FakeDataTrack::default());
    dt.pub_handle_returns(pub_handle);
    dt.to_proto_returns(livekit::DataTrackInfo {
        pub_handle: pub_handle as u32,
        sid: format!("DTR_{name}"),
        name: name.to_string(),
        ..Default::default()
    });
    dt
}

fn is_track(dt: Option<Arc<dyn types::DataTrack>>, expected: &Arc<FakeDataTrack>) -> bool {
    dt.is_some_and(|dt| std::ptr::addr_eq(Arc::as_ptr(&dt), Arc::as_ptr(expected)))
}

// The manager keeps one track per handle and does not refuse a second one under a handle
// that is taken: the second takes the place of the first, which is not closed (the request
// to publish is refused before it gets here, by the participant).
#[test]
fn the_manager_keeps_the_published_data_tracks_by_handle() {
    let listener = Arc::new(FakeParticipantListener::default());
    let p = Arc::new(FakeParticipant::default());
    p.get_participant_listener_returns(listener.clone());
    let participant: Arc<dyn types::Participant> = p.clone();
    let u = UpDataTrackManager::new(UpDataTrackManagerParams {
        logger: logger::get_logger(),
        participant: Arc::downgrade(&participant),
    });

    let first = new_data_track(5, "first");
    let second = new_data_track(5, "second");
    let other = new_data_track(6, "other");

    u.add_published_data_track(first.clone());
    assert_eq!(1, u.get_published_data_tracks().len());
    assert_eq!(1, listener.on_data_track_published_call_count());
    assert!(is_track(u.get_published_data_track(5), &first));

    // a second track under the same handle
    u.add_published_data_track(second.clone());
    assert_eq!(1, u.get_published_data_tracks().len());
    assert_eq!(2, listener.on_data_track_published_call_count());
    assert!(is_track(u.get_published_data_track(5), &second));
    assert_eq!(0, first.close_call_count());
    // the listener is told of whose track, and of which
    let (of, track) = listener.on_data_track_published_args_for_call(1);
    assert!(Arc::ptr_eq(&of, &participant));
    assert!(is_track(Some(track), &second));

    u.add_published_data_track(other.clone());
    assert_eq!(2, u.get_published_data_tracks().len());
    let mut names: Vec<String> = u.to_proto().into_iter().map(|dti| dti.name).collect();
    names.sort();
    assert_eq!(vec!["other", "second"], names);
    assert!(u.get_published_data_track(9).is_none());

    // a packet goes to the track of its handle, one of no track nowhere
    let packet_of = |handle: u16| datatrack::Packet {
        header: datatrack::Header {
            handle,
            ..Default::default()
        },
        ..Default::default()
    };
    u.handle_received_data_track_message(&[1, 2, 3], &packet_of(5), 77);
    u.handle_received_data_track_message(&[1, 2, 3], &packet_of(9), 78);
    assert_eq!(0, first.handle_packet_call_count());
    assert_eq!(1, second.handle_packet_call_count());
    assert_eq!(0, other.handle_packet_call_count());
    let (data, handled, arrival_time) = second.handle_packet_args_for_call(0);
    assert_eq!(vec![1, 2, 3], data);
    assert_eq!(5, handled.handle);
    assert_eq!(77, arrival_time);

    // only the track that is there under its handle is removed, closed and told of
    u.remove_published_data_track(first.clone());
    assert_eq!(2, u.get_published_data_tracks().len());
    assert_eq!(0, first.close_call_count());
    assert_eq!(0, listener.on_data_track_unpublished_call_count());
    u.remove_published_data_track(second.clone());
    assert_eq!(1, u.get_published_data_tracks().len());
    assert_eq!(1, second.close_call_count());
    assert_eq!(1, listener.on_data_track_unpublished_call_count());
    let (of, track) = listener.on_data_track_unpublished_args_for_call(0);
    assert!(Arc::ptr_eq(&of, &participant));
    assert!(is_track(Some(track), &second));
    u.remove_published_data_track(second.clone());
    assert_eq!(1, second.close_call_count());
    assert_eq!(1, listener.on_data_track_unpublished_call_count());

    // closing closes what is left, once, and tells nobody
    u.close();
    assert!(u.get_published_data_tracks().is_empty());
    assert_eq!(1, other.close_call_count());
    assert_eq!(1, listener.on_data_track_unpublished_call_count());
    u.close();
    assert_eq!(1, other.close_call_count());

    // a closed manager still takes a track
    let late = new_data_track(8, "late");
    u.add_published_data_track(late.clone());
    assert_eq!(1, u.get_published_data_tracks().len());
    assert_eq!(4, listener.on_data_track_published_call_count());
    assert_eq!(0, late.close_call_count());
}

// The manager holds its participant weakly: without one, tracks come and go and no listener
// is told.
#[test]
fn a_manager_whose_participant_is_gone_tells_nobody() {
    let listener = Arc::new(FakeParticipantListener::default());
    let p = Arc::new(FakeParticipant::default());
    p.get_participant_listener_returns(listener.clone());
    let participant: Arc<dyn types::Participant> = p.clone();
    let u = UpDataTrackManager::new(UpDataTrackManagerParams {
        logger: logger::get_logger(),
        participant: Arc::downgrade(&participant),
    });
    let weak = Arc::downgrade(&p);
    drop(participant);
    drop(p);
    assert!(weak.upgrade().is_none());

    let dt = new_data_track(5, "first");
    u.add_published_data_track(dt.clone());
    assert!(is_track(u.get_published_data_track(5), &dt));
    u.remove_published_data_track(dt.clone());
    assert!(u.get_published_data_track(5).is_none());
    assert_eq!(1, dt.close_call_count());
    assert_eq!(0, listener.on_data_track_published_call_count());
    assert_eq!(0, listener.on_data_track_unpublished_call_count());
}
