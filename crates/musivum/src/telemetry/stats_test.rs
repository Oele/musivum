// Ported from LiveKit, pkg/telemetry/stats_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The fixture, which `events_test.rs` shares as upstream's
// tests of the package share it, gives the service counterfeiter's `FakeAnalyticsService` of
// the trait's fake; its arguments are the stats alone (no context). `prometheus.Init` has no
// counterpart: the collectors are statics. A guard is an `Arc` the service is handed. The
// sleeps are the runtime's.

use std::sync::Arc;

use super::*;
use crate::go::time::{self, MILLISECOND};
use crate::telemetry::{
    FakeAnalyticsService, ReferenceGuard, TelemetryService, new_telemetry_service,
};

// func init() { prometheus.Init("test", livekit.NodeType_SERVER) }: see the header

pub(crate) struct TelemetryServiceFixture {
    pub(crate) sut: Arc<dyn TelemetryService>,
    pub(crate) analytics: Arc<FakeAnalyticsService>,
}

pub(crate) fn create_fixture() -> TelemetryServiceFixture {
    let analytics = Arc::new(FakeAnalyticsService::default());
    let sut = new_telemetry_service(None, analytics.clone());
    TelemetryServiceFixture { sut, analytics }
}

fn guard() -> Option<Arc<ReferenceGuard>> {
    Some(Arc::new(ReferenceGuard::default()))
}

fn room() -> livekit::Room {
    livekit::Room {
        sid: "RoomSid".to_string(),
        name: "RoomName".to_string(),
        ..Default::default()
    }
}

fn stream(primary_bytes: u64, primary_packets: u32) -> livekit::AnalyticsStream {
    livekit::AnalyticsStream {
        primary_bytes,
        primary_packets,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_participant_and_room_data_are_sent_with_analytics() {
    let fixture = create_fixture();

    // prepare
    let room = room();
    let part_sid = livekit::ParticipantId::from("part1");
    let client_info = livekit::ClientInfo {
        sdk: 2,
        ..Default::default()
    };
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    fixture.sut.participant_joined(
        &room,
        &participant_info,
        Some(&client_info),
        None,
        true,
        guard(),
    );

    // do
    let packet = 33;
    let stat = livekit::AnalyticsStat {
        streams: vec![stream(packet, 0)],
        ..Default::default()
    };
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Downstream,
            part_sid.clone(),
            "".into(),
        ),
        &stat,
    );

    // flush
    fixture.flush().await;

    // test
    assert_eq!(1, fixture.analytics.send_stats_call_count());
    let stats = fixture.analytics.send_stats_args_for_call(0);
    assert_eq!(1, stats.len());
    assert_eq!(livekit::StreamType::Downstream, stats[0].kind());
    assert_eq!(part_sid.to_string(), stats[0].participant_id);
    assert_eq!(room.sid, stats[0].room_id);
    assert_eq!(room.name, stats[0].room_name);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_downstream_packets() {
    let fixture = create_fixture();

    // prepare
    let room = room();
    let part_sid = livekit::ParticipantId::from("part1");
    let client_info = livekit::ClientInfo {
        sdk: 2,
        ..Default::default()
    };
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    fixture.sut.participant_joined(
        &room,
        &participant_info,
        Some(&client_info),
        None,
        true,
        guard(),
    );

    // do
    let packets = [33u64, 23];
    let total_bytes = packets[0] + packets[1];
    let total_packets = packets.len();
    let track_id = livekit::TrackId::from("trackID");
    for packet in packets {
        let stat = livekit::AnalyticsStat {
            streams: vec![stream(packet, 1)],
            ..Default::default()
        };
        fixture.sut.track_stats(
            &livekit::RoomId::from(&room.sid),
            &livekit::RoomName::from(&room.name),
            &stats_key_for_data(
                "test",
                livekit::StreamType::Downstream,
                part_sid.clone(),
                track_id.clone(),
            ),
            &stat,
        );
    }

    // flush
    fixture.flush().await;

    // test
    assert_eq!(1, fixture.analytics.send_stats_call_count());
    let stats = fixture.analytics.send_stats_args_for_call(0);
    assert_eq!(1, stats.len());
    assert_eq!(livekit::StreamType::Downstream, stats[0].kind());
    assert_eq!(total_bytes, stats[0].streams[0].primary_bytes);
    assert_eq!(total_packets, stats[0].streams[0].primary_packets as usize);
    assert_eq!(track_id.to_string(), stats[0].track_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_downstream_packets_several_tracks() {
    let fixture = create_fixture();

    // prepare
    let room = room();
    let part_sid = livekit::ParticipantId::from("part1");
    let client_info = livekit::ClientInfo {
        sdk: 2,
        ..Default::default()
    };
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    fixture.sut.participant_joined(
        &room,
        &participant_info,
        Some(&client_info),
        None,
        true,
        guard(),
    );

    // do
    let packet1 = 33;
    let track_id1 = livekit::TrackId::from("trackID1");
    let stat1 = livekit::AnalyticsStat {
        streams: vec![stream(packet1, 1)],
        ..Default::default()
    };
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Downstream,
            part_sid.clone(),
            track_id1.clone(),
        ),
        &stat1,
    );

    let packet2 = 23;
    let track_id2 = livekit::TrackId::from("trackID2");
    let stat2 = livekit::AnalyticsStat {
        streams: vec![stream(packet2, 1)],
        ..Default::default()
    };
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Downstream,
            part_sid.clone(),
            track_id2.clone(),
        ),
        &stat2,
    );

    // flush
    fixture.flush().await;

    // test
    assert_eq!(1, fixture.analytics.send_stats_call_count());
    let stats = fixture.analytics.send_stats_args_for_call(0);
    assert_eq!(2, stats.len());

    let mut found1 = false;
    let mut found2 = false;
    for sent_stat in &stats {
        if livekit::TrackId::from(&sent_stat.track_id) == track_id1 {
            found1 = true;
            assert_eq!(packet1, sent_stat.streams[0].primary_bytes);
            assert_eq!(1, sent_stat.streams[0].primary_packets);
        } else if livekit::TrackId::from(&sent_stat.track_id) == track_id2 {
            found2 = true;
            assert_eq!(packet2, sent_stat.streams[0].primary_bytes);
            assert_eq!(1, sent_stat.streams[0].primary_packets);
        }
    }
    assert!(found1);
    assert!(found2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_down_stream_stat() {
    let fixture = create_fixture();

    // prepare
    let room = room();
    let part_sid = livekit::ParticipantId::from("part1");
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    fixture
        .sut
        .participant_joined(&room, &participant_info, None, None, true, guard());

    // do
    let stat1 = livekit::AnalyticsStat {
        streams: vec![livekit::AnalyticsStream {
            primary_bytes: 1,
            primary_packets: 1,
            packets_lost: 3,
            nacks: 1,
            plis: 1,
            rtt: 23,
            jitter: 3,
            ..Default::default()
        }],
        ..Default::default()
    };
    let track_id = livekit::TrackId::from("trackID1");
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Downstream,
            part_sid.clone(),
            track_id.clone(),
        ),
        &stat1,
    );

    let stat2 = livekit::AnalyticsStat {
        streams: vec![livekit::AnalyticsStream {
            primary_bytes: 2,
            primary_packets: 2,
            packets_lost: 4,
            nacks: 1,
            plis: 1,
            firs: 1,
            rtt: 10,
            jitter: 5,
            ..Default::default()
        }],
        ..Default::default()
    };
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Downstream,
            part_sid.clone(),
            track_id.clone(),
        ),
        &stat2,
    );

    // flush
    fixture.flush().await;

    // test
    assert_eq!(1, fixture.analytics.send_stats_call_count());
    let stats = fixture.analytics.send_stats_args_for_call(0);
    assert_eq!(1, stats.len());
    assert_eq!(livekit::StreamType::Downstream, stats[0].kind());
    assert_eq!(2, stats[0].streams[0].nacks);
    assert_eq!(2, stats[0].streams[0].plis);
    assert_eq!(1, stats[0].streams[0].firs);
    assert_eq!(23, stats[0].streams[0].rtt); // max of RTT
    assert_eq!(5, stats[0].streams[0].jitter); // max of jitter
    assert_eq!(7, stats[0].streams[0].packets_lost); // coalesced delta packet losses
    assert_eq!(track_id.to_string(), stats[0].track_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_packet_lost_diff_should_be_sent_to_telemetry() {
    let fixture = create_fixture();

    // prepare
    let room = room();
    let part_sid = livekit::ParticipantId::from("part1");
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    fixture
        .sut
        .participant_joined(&room, &participant_info, None, None, true, guard());

    // do
    let track_id = livekit::TrackId::from("trackID1");
    let stat1 = livekit::AnalyticsStat {
        streams: vec![livekit::AnalyticsStream {
            primary_bytes: 1,
            primary_packets: 1,
            packets_lost: 1,
            ..Default::default()
        }],
        ..Default::default()
    };
    // there should be bytes reported so that stats are sent
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Downstream,
            part_sid.clone(),
            track_id.clone(),
        ),
        &stat1,
    );

    // flush
    fixture.flush().await;

    let stat2 = livekit::AnalyticsStat {
        streams: vec![livekit::AnalyticsStream {
            primary_bytes: 2,
            primary_packets: 2,
            packets_lost: 4,
            ..Default::default()
        }],
        ..Default::default()
    };
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Downstream,
            part_sid.clone(),
            track_id.clone(),
        ),
        &stat2,
    );

    // flush
    fixture.flush().await;

    // test
    assert_eq!(2, fixture.analytics.send_stats_call_count()); // 2 calls to fixture.sut.FlushStats()
    let stats = fixture.analytics.send_stats_args_for_call(0);
    assert_eq!(1, stats.len());
    assert_eq!(livekit::StreamType::Downstream, stats[0].kind());
    assert_eq!(1, stats[0].streams[0].packets_lost); // see pkts1

    let stats = fixture.analytics.send_stats_args_for_call(1);
    assert_eq!(1, stats.len());
    assert_eq!(livekit::StreamType::Downstream, stats[0].kind());
    assert_eq!(4, stats[0].streams[0].packets_lost); // delta loss should be sent as is
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_down_stream_rtcp_several_tracks() {
    let fixture = create_fixture();

    // prepare
    let room = room();
    let part_sid = livekit::ParticipantId::from("part1");
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    fixture
        .sut
        .participant_joined(&room, &participant_info, None, None, true, guard());

    // do
    let track_id1 = livekit::TrackId::from("trackID1");
    let track_id2 = livekit::TrackId::from("trackID2");
    let stat1 = livekit::AnalyticsStat {
        streams: vec![stream(1, 1)],
        ..Default::default()
    };
    // there should be bytes reported so that stats are sent
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Downstream,
            part_sid.clone(),
            track_id1.clone(),
        ),
        &stat1,
    );

    let stat2 = livekit::AnalyticsStat {
        streams: vec![livekit::AnalyticsStream {
            primary_bytes: 2,
            primary_packets: 2,
            nacks: 1,
            ..Default::default()
        }],
        ..Default::default()
    };
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Downstream,
            part_sid.clone(),
            track_id1.clone(),
        ),
        &stat2,
    );

    let stat3 = livekit::AnalyticsStat {
        streams: vec![livekit::AnalyticsStream {
            primary_bytes: 3,
            primary_packets: 3,
            firs: 1,
            ..Default::default()
        }],
        ..Default::default()
    };
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Downstream,
            part_sid.clone(),
            track_id2.clone(),
        ),
        &stat3,
    );

    // flush
    fixture.flush().await;

    // test
    assert_eq!(1, fixture.analytics.send_stats_call_count());
    let stats = fixture.analytics.send_stats_args_for_call(0);
    assert_eq!(2, stats.len());

    let mut found1 = false;
    let mut found2 = false;
    for sent_stat in &stats {
        if livekit::TrackId::from(&sent_stat.track_id) == track_id1 {
            found1 = true;
            assert_eq!(livekit::StreamType::Downstream, sent_stat.kind());
            assert_eq!(1, sent_stat.streams[0].nacks); // see pkts1 above
        } else if livekit::TrackId::from(&sent_stat.track_id) == track_id2 {
            found2 = true;
            assert_eq!(livekit::StreamType::Downstream, sent_stat.kind());
            assert_eq!(1, sent_stat.streams[0].firs); // see pkts2 above
        }
    }
    assert!(found1);
    assert!(found2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_upstream_stat() {
    let fixture = create_fixture();

    // prepare
    let room = room();
    let part_sid = livekit::ParticipantId::from("part1");
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    fixture
        .sut
        .participant_joined(&room, &participant_info, None, None, true, guard());

    // do
    let stat1 = livekit::AnalyticsStat {
        streams: vec![livekit::AnalyticsStream {
            primary_bytes: 1,
            primary_packets: 1,
            packets_lost: 3,
            nacks: 1,
            plis: 1,
            firs: 1,
            rtt: 13,
            jitter: 5,
            ..Default::default()
        }],
        ..Default::default()
    };
    let track_id = livekit::TrackId::from("trackID");

    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Upstream,
            part_sid.clone(),
            track_id.clone(),
        ),
        &stat1,
    );

    let stat2 = livekit::AnalyticsStat {
        streams: vec![livekit::AnalyticsStream {
            primary_bytes: 2,
            primary_packets: 2,
            packets_lost: 4,
            nacks: 1,
            plis: 1,
            firs: 1,
            rtt: 33,
            jitter: 2,
            ..Default::default()
        }],
        ..Default::default()
    };
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Upstream,
            part_sid.clone(),
            track_id.clone(),
        ),
        &stat2,
    );

    // flush
    fixture.flush().await;

    // test
    assert_eq!(1, fixture.analytics.send_stats_call_count());
    let stats = fixture.analytics.send_stats_args_for_call(0);
    assert_eq!(1, stats.len());
    assert_eq!(livekit::StreamType::Upstream, stats[0].kind());
    assert_eq!(2, stats[0].streams[0].nacks);
    assert_eq!(2, stats[0].streams[0].plis);
    assert_eq!(2, stats[0].streams[0].firs);
    assert_eq!(33, stats[0].streams[0].rtt); // max of RTT
    assert_eq!(5, stats[0].streams[0].jitter); // max of jitter
    assert_eq!(7, stats[0].streams[0].packets_lost); // coalesced delta packet losses
    assert_eq!(track_id.to_string(), stats[0].track_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_on_upstream_rtcp_several_tracks() {
    let fixture = create_fixture();

    // prepare
    let room = room();
    let part_sid = livekit::ParticipantId::from("part1");
    let identity = livekit::ParticipantIdentity::from("part1Identity");
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        identity: identity.to_string(),
        ..Default::default()
    };
    fixture
        .sut
        .participant_joined(&room, &participant_info, None, None, true, guard());

    // there should be bytes reported so that stats are sent
    let mut total_bytes = 1u64;
    let mut total_packets = 1u32;
    let track_id1 = livekit::TrackId::from("trackID1");
    let track_id2 = livekit::TrackId::from("trackID2");

    let stat1 = livekit::AnalyticsStat {
        streams: vec![stream(total_bytes, total_packets)],
        ..Default::default()
    };
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Upstream,
            part_sid.clone(),
            track_id1.clone(),
        ),
        &stat1,
    );
    // using same buffer is not correct but for test it is fine
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Upstream,
            part_sid.clone(),
            track_id2.clone(),
        ),
        &stat1,
    );

    // do
    total_bytes += 1;
    total_packets += 1;
    let stat2 = livekit::AnalyticsStat {
        streams: vec![livekit::AnalyticsStream {
            primary_bytes: total_bytes,
            primary_packets: total_packets,
            nacks: 1,
            ..Default::default()
        }],
        ..Default::default()
    };
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Upstream,
            part_sid.clone(),
            track_id1.clone(),
        ),
        &stat2,
    );

    let stat3 = livekit::AnalyticsStat {
        streams: vec![livekit::AnalyticsStream {
            primary_bytes: total_bytes,
            primary_packets: total_packets,
            firs: 1,
            ..Default::default()
        }],
        ..Default::default()
    };
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Upstream,
            part_sid.clone(),
            track_id2.clone(),
        ),
        &stat3,
    );

    // flush
    fixture.flush().await;

    // test
    assert_eq!(1, fixture.analytics.send_stats_call_count());
    let stats = fixture.analytics.send_stats_args_for_call(0);
    assert_eq!(2, stats.len());

    let mut found1 = false;
    let mut found2 = false;
    for sent_stat in &stats {
        if livekit::TrackId::from(&sent_stat.track_id) == track_id1 {
            found1 = true;
            assert_eq!(livekit::StreamType::Upstream, sent_stat.kind());
            assert_eq!(1, sent_stat.streams[0].nacks); // see pkts1 above
        } else if livekit::TrackId::from(&sent_stat.track_id) == track_id2 {
            found2 = true;
            assert_eq!(livekit::StreamType::Upstream, sent_stat.kind());
            assert_eq!(1, sent_stat.streams[0].firs); // see pkts2 above
        }
        assert_eq!(3, sent_stat.streams[0].primary_bytes);
        assert_eq!(3, sent_stat.streams[0].primary_packets);
    }
    assert!(found1);
    assert!(found2);

    // remove 1 track - track stats were flushed above, so no more calls to SendStats
    fixture.sut.track_unpublished(
        &room,
        &part_sid,
        &identity,
        &livekit::TrackInfo {
            sid: track_id2.to_string(),
            ..Default::default()
        },
        true,
        true,
    );

    // flush
    fixture.flush().await;

    assert_eq!(1, fixture.analytics.send_stats_call_count());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_analytics_sent_when_participant_leaves() {
    let fixture = create_fixture();

    // prepare
    let room = livekit::Room::default();
    let part_sid = "part1";
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    let guard = guard();
    fixture
        .sut
        .participant_joined(&room, &participant_info, None, None, true, guard.clone());

    // do
    fixture
        .sut
        .participant_left(&room, &participant_info, true, guard);

    // should not be called if there are no track stats
    time::sleep(500 * MILLISECOND).await;
    assert_eq!(0, fixture.analytics.send_stats_call_count());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_up_track() {
    let fixture = create_fixture();

    // prepare
    let room = room();
    let part_sid = livekit::ParticipantId::from("part1");
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    fixture
        .sut
        .participant_joined(&room, &participant_info, None, None, true, guard());

    // do
    let total_bytes: u64 = 3;
    let total_packets: u32 = 3;

    let stat = livekit::AnalyticsStat {
        streams: vec![stream(total_bytes, total_packets)],
        ..Default::default()
    };
    let track_id = livekit::TrackId::from("trackID");
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Upstream,
            part_sid.clone(),
            track_id.clone(),
        ),
        &stat,
    );

    // flush
    fixture.flush().await;

    // test
    assert_eq!(1, fixture.analytics.send_stats_call_count());
    let stats = fixture.analytics.send_stats_args_for_call(0);
    assert_eq!(1, stats.len());
    assert_eq!(livekit::StreamType::Upstream, stats[0].kind());
    assert_eq!(total_bytes, stats[0].streams[0].primary_bytes);
    assert_eq!(total_packets, stats[0].streams[0].primary_packets);
    assert_eq!(track_id.to_string(), stats[0].track_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_up_track_several_buffers_simulcast() {
    let fixture = create_fixture();

    // prepare
    let room = room();
    let part_sid = livekit::ParticipantId::from("part1");
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    fixture
        .sut
        .participant_joined(&room, &participant_info, None, None, true, guard());

    // do
    let track_id = livekit::TrackId::from("trackID");
    let stat1 = livekit::AnalyticsStat {
        streams: vec![stream(1, 1), stream(2, 2)],
        ..Default::default()
    };
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Upstream,
            part_sid.clone(),
            track_id.clone(),
        ),
        &stat1,
    );

    // flush
    fixture.flush().await;

    // test
    assert_eq!(1, fixture.analytics.send_stats_call_count());
    let stats = fixture.analytics.send_stats_args_for_call(0);
    assert_eq!(1, stats.len());
    assert_eq!(livekit::StreamType::Upstream, stats[0].kind());
    // should be a consolidated stream
    assert_eq!(
        stat1.streams[0].primary_bytes + stat1.streams[1].primary_bytes,
        stats[0].streams[0].primary_bytes
    );
    assert_eq!(
        stat1.streams[0].primary_packets + stat1.streams[1].primary_packets,
        stats[0].streams[0].primary_packets
    );
    assert_eq!(track_id.to_string(), stats[0].track_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_both_downstream_and_upstream_stats_are_sent_together() {
    let fixture = create_fixture();

    // prepare
    let room = room();
    let part_sid = livekit::ParticipantId::from("part1");
    let participant_info = livekit::ParticipantInfo {
        sid: part_sid.to_string(),
        ..Default::default()
    };
    fixture
        .sut
        .participant_joined(&room, &participant_info, None, None, true, guard());

    // do
    // upstream bytes
    let stat1 = livekit::AnalyticsStat {
        streams: vec![stream(3, 3)],
        ..Default::default()
    };
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Upstream,
            part_sid.clone(),
            "trackID".into(),
        ),
        &stat1,
    );
    // downstream bytes
    let stat2 = livekit::AnalyticsStat {
        streams: vec![stream(1, 1)],
        ..Default::default()
    };
    fixture.sut.track_stats(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
        &stats_key_for_data(
            "test",
            livekit::StreamType::Downstream,
            part_sid.clone(),
            "trackID1".into(),
        ),
        &stat2,
    );

    // flush
    fixture.flush().await;

    // test
    assert_eq!(1, fixture.analytics.send_stats_call_count());
    let stats = fixture.analytics.send_stats_args_for_call(0);
    assert_eq!(2, stats.len());
    assert_eq!(livekit::StreamType::Upstream, stats[0].kind());
    assert_eq!(livekit::StreamType::Downstream, stats[1].kind());
}

impl TelemetryServiceFixture {
    pub(crate) async fn flush(&self) {
        time::sleep(500 * MILLISECOND).await;
        self.sut.flush_stats();
    }
}
