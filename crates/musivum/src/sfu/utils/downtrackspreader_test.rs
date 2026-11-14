// Ported from LiveKit, pkg/sfu/utils/downtrackspreader_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `accept` of the test sender is atomic, because the
// test changes it after the sender has been stored and is shared; slices of senders are
// compared by identity (`Arc::ptr_eq`), which is what the original's pointers stand for. The
// allocation count of `TestBroadcastRTP` is not checked, see there.

use super::*;

#[allow(dead_code)]
struct TestPacket {
    seq: i64,
}

#[derive(Default)]
struct TestSender {
    id: ParticipantId,
    writes: atomic::Int32,
    accept: atomic::Int32,
}

impl Sender for TestSender {
    fn subscriber_id(&self) -> ParticipantId {
        self.id.clone()
    }
}

impl RtpWriter<TestPacket> for TestSender {
    fn write_rtp(&self, _pkt: &TestPacket, _layer: i32) -> i32 {
        self.writes.add(1);
        self.accept.load()
    }
}

fn new_test_spreader(
    num_down_tracks: usize,
    threshold: usize,
) -> (DownTrackSpreader<Arc<TestSender>>, Vec<Arc<TestSender>>) {
    let d = DownTrackSpreader::<Arc<TestSender>>::new(DownTrackSpreaderParams {
        threshold,
        ..Default::default()
    });
    let mut senders = Vec::with_capacity(num_down_tracks);
    for i in 0..num_down_tracks {
        senders.push(Arc::new(TestSender {
            id: ParticipantId::from(format!("p{i}")),
            accept: atomic::Int32::new(1),
            ..Default::default()
        }));
        d.store(senders[i].clone());
    }
    (d, senders)
}

// require.Equal on two slices of pointers.
#[track_caller]
fn assert_same_senders(expected: &[Arc<TestSender>], actual: &[Arc<TestSender>]) {
    assert_eq!(expected.len(), actual.len());
    for (e, a) in expected.iter().zip(actual.iter()) {
        assert!(Arc::ptr_eq(e, a), "expected {}, got {}", e.id, a.id);
    }
}

#[test]
fn test_broadcast_rtp() {
    struct TestCase {
        name: &'static str,
        num_down_tracks: usize,
        threshold: usize,
        // The allocations the original allows: not checked here, see below.
        #[allow(dead_code)]
        max_allocs: f64,
    }
    for tc in [
        TestCase {
            name: "serial",
            num_down_tracks: 5,
            threshold: 20,
            max_allocs: 0.0,
        },
        TestCase {
            name: "parallel",
            num_down_tracks: 50,
            threshold: 20,
            max_allocs: 2.0, // shared state and worker funcval
        },
    ] {
        let (d, senders) = new_test_spreader(tc.num_down_tracks, tc.threshold);
        senders[0].accept.store(0); // one down track that drops
        let pkt = TestPacket { seq: 0 };

        let written = broadcast_rtp(&d, &pkt, 2);
        assert_eq!((tc.num_down_tracks - 1) as i32, written, "{}", tc.name);
        for s in &senders {
            assert_eq!(1, s.writes.load(), "{}", tc.name);
        }

        // The original goes on, when the race detector is off, to measure the allocations
        // of a call with `testing.AllocsPerRun(1000, ...)` and requires them to be at most
        // `maxAllocs`. That cannot be ported as it is: counting allocations takes a global
        // allocator of the test's own, which is `unsafe` to write, and the workspace forbids
        // that. The runs themselves are kept (a warm-up and 1000 measured ones upstream).
        for _ in 0..1001 {
            broadcast_rtp(&d, &pkt, 2);
        }
    }
}

#[test]
fn test_broadcast_rtp_empty() {
    let d = DownTrackSpreader::<Arc<TestSender>>::new(DownTrackSpreaderParams {
        threshold: 20,
        ..Default::default()
    });
    assert_eq!(0, broadcast_rtp(&d, &TestPacket { seq: 0 }, 0));
}

#[test]
fn test_down_track_spreader_reset_remains_reusable() {
    let spreader = DownTrackSpreader::<Arc<TestSender>>::new(DownTrackSpreaderParams::default());
    let first = Arc::new(TestSender {
        id: "first".into(),
        ..Default::default()
    });
    let second = Arc::new(TestSender {
        id: "second".into(),
        ..Default::default()
    });

    spreader.store(first.clone());
    assert_same_senders(&[first], &spreader.reset_and_get_down_tracks());
    assert_eq!(0, spreader.down_track_count());

    spreader.store(second.clone());
    assert_same_senders(&[second], &spreader.get_down_tracks());
}

#[test]
fn test_down_track_spreader_rejects_store_after_close() {
    let spreader = DownTrackSpreader::<Arc<TestSender>>::new(DownTrackSpreaderParams::default());
    let first = Arc::new(TestSender {
        id: "first".into(),
        ..Default::default()
    });
    let late = Arc::new(TestSender {
        id: "late".into(),
        ..Default::default()
    });

    spreader.store(first.clone());
    assert_same_senders(&[first], &spreader.close_and_get_down_tracks());
    assert_eq!(0, spreader.down_track_count());

    assert!(!spreader.try_store(late));
    assert_eq!(0, spreader.down_track_count());
}
