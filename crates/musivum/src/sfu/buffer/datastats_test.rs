// Ported from LiveKit, pkg/sfu/buffer/datastats_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `proto.Equal` is `==` on the messages.

use super::*;
use crate::testutils::assert_in_delta;

#[tokio::test(flavor = "multi_thread")]
async fn test_data_stats() {
    let stats = DataStats::new(DataStatsParam {
        window_duration: time::SECOND,
    });

    time::sleep(time::MILLISECOND).await;
    let mut r = stats.to_proto_aggregate_only();
    assert_eq!(
        time::timestamppb_as_time(r.start_time.as_ref().unwrap()).unix_nano(),
        stats.inner.read().start_time.unix_nano()
    );
    assert!(r.end_time.is_some());
    assert_ne!(0.0, r.duration);
    r.start_time = None;
    r.end_time = None;
    r.duration = 0.0;
    assert_eq!(r, livekit::RtpStats::default());

    stats.update(100, time::now().unix_nano());
    let r = stats.to_proto_active();
    assert_eq!(100, r.bytes);
    assert_ne!(0.0, r.bitrate);

    // wait for window duration
    time::sleep(time::SECOND).await;
    let r = stats.to_proto_active();
    assert_eq!(r, livekit::RtpStats::default());
    stats.stop();
    let r = stats.to_proto_aggregate_only();
    assert_eq!(100, r.bytes);
    assert_ne!(0.0, r.bitrate);
}

#[test]
fn test_data_stats_active_bitrate_and_duration() {
    let stats = DataStats::new(DataStatsParam {
        window_duration: 10 * time::SECOND,
    });

    // 1000 bytes over a 1.5 second window is 8000 bits / 1.5 s
    {
        let mut s = stats.inner.write();
        s.window_start = time::now().add(-1500 * time::MILLISECOND).unix_nano();
        s.window_bytes = 1000;
    }

    let r = stats.to_proto_active();
    assert_eq!(1000, r.bytes);
    assert_in_delta(1.5, r.duration, 0.1);
    assert_in_delta(8000.0 / 1.5, r.bitrate, 0.1 * 8000.0 / 1.5);
}
