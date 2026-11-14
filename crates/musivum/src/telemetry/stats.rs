// Ported from LiveKit, pkg/telemetry/stats.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. `TrackStats` is a method of `TelemetryServiceImpl`
// (`telemetryService` upstream), which its implementation of the interface hands on to
// (`telemetryservice.rs`); its job gets the service and copies of the key and the stat. The
// key's fields are visible in this module. The key can be compared, as a Go struct can. The
// sums of a stat's streams are `uint32` and wrap as upstream. The test at the end is new; the
// original's tests are in `stats_test.rs`.

use super::prometheus;
use super::telemetryservice::TelemetryServiceImpl;
use crate::protocol::livekit;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct StatsKey {
    pub(super) country: String,
    pub(super) stream_type: livekit::StreamType,
    pub(super) participant_id: livekit::ParticipantId,
    pub(super) track_id: livekit::TrackId,
    pub(super) track_source: livekit::TrackSource,
    pub(super) track_type: livekit::TrackType,
    pub(super) track: bool,
}

pub fn stats_key_for_track(
    country: &str,
    stream_type: livekit::StreamType,
    participant_id: livekit::ParticipantId,
    track_id: livekit::TrackId,
    track_source: livekit::TrackSource,
    track_type: livekit::TrackType,
) -> StatsKey {
    StatsKey {
        country: country.to_string(),
        stream_type,
        participant_id,
        track_id,
        track_source,
        track_type,
        track: true,
    }
}

pub fn stats_key_for_data(
    country: &str,
    stream_type: livekit::StreamType,
    participant_id: livekit::ParticipantId,
    track_id: livekit::TrackId,
) -> StatsKey {
    StatsKey {
        country: country.to_string(),
        stream_type,
        participant_id,
        track_id,
        ..Default::default()
    }
}

impl StatsKey {
    pub fn stream_type(&self) -> livekit::StreamType {
        self.stream_type
    }
}

impl TelemetryServiceImpl {
    pub(super) fn track_stats(
        &self,
        room_id: &livekit::RoomId,
        _room_name: &livekit::RoomName,
        key: &StatsKey,
        stat: &livekit::AnalyticsStat,
    ) {
        let (room_id, key, stat) = (room_id.clone(), key.clone(), stat.clone());
        self.enqueue(move |t| {
            let mut direction = prometheus::Direction::Incoming;
            if key.stream_type == livekit::StreamType::Downstream {
                direction = prometheus::Direction::Outgoing;
            }

            let mut nacks = 0u32;
            let mut plis = 0u32;
            let mut firs = 0u32;
            let mut packets = 0u32;
            let mut bytes = 0u64;
            let mut retransmit_bytes = 0u64;
            let mut retransmit_packets = 0u32;
            for stream in &stat.streams {
                nacks = nacks.wrapping_add(stream.nacks);
                plis = plis.wrapping_add(stream.plis);
                firs = firs.wrapping_add(stream.firs);
                packets = packets
                    .wrapping_add(stream.primary_packets.wrapping_add(stream.padding_packets));
                bytes = bytes.wrapping_add(stream.primary_bytes.wrapping_add(stream.padding_bytes));
                if key.stream_type == livekit::StreamType::Downstream {
                    retransmit_packets = retransmit_packets.wrapping_add(stream.retransmit_packets);
                    retransmit_bytes = retransmit_bytes.wrapping_add(stream.retransmit_bytes);
                } else {
                    // for upstream, we don't account for these separately for now
                    packets = packets.wrapping_add(stream.retransmit_packets);
                    bytes = bytes.wrapping_add(stream.retransmit_bytes);
                }
                if key.track {
                    prometheus::record_packet_loss(
                        &key.country,
                        direction,
                        key.track_source,
                        key.track_type,
                        stream.packets_lost,
                        stream.primary_packets.wrapping_add(stream.padding_packets),
                    );
                    prometheus::record_packet_out_of_order(
                        &key.country,
                        direction,
                        key.track_source,
                        key.track_type,
                        stream.packets_out_of_order,
                        stream.primary_packets.wrapping_add(stream.padding_packets),
                    );
                    prometheus::record_rtt(
                        &key.country,
                        direction,
                        key.track_source,
                        key.track_type,
                        stream.rtt,
                    );
                    prometheus::record_jitter(
                        &key.country,
                        direction,
                        key.track_source,
                        key.track_type,
                        stream.jitter,
                    );
                }
            }
            prometheus::increment_rtcp(&key.country, direction, nacks, plis, firs);
            prometheus::increment_packets(&key.country, direction, packets as u64, false);
            prometheus::increment_bytes(&key.country, direction, bytes, false);
            if retransmit_packets != 0 {
                prometheus::increment_packets(
                    &key.country,
                    direction,
                    retransmit_packets as u64,
                    true,
                );
            }
            if retransmit_bytes != 0 {
                prometheus::increment_bytes(&key.country, direction, retransmit_bytes, true);
            }

            if let Some(worker) = t.get_worker(&room_id, &key.participant_id) {
                worker.on_track_stat(key.track_id.clone(), key.stream_type, stat);
            }
        });
    }
}

#[cfg(test)]
#[path = "stats_test.rs"]
pub(super) mod stats_test;

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum.
    #[test]
    fn a_key_for_a_track_has_what_it_was_made_of_and_one_for_data_no_track() {
        let key = stats_key_for_track(
            "NL",
            livekit::StreamType::Downstream,
            "PA_1".into(),
            "TR_1".into(),
            livekit::TrackSource::Camera,
            livekit::TrackType::Video,
        );
        assert_eq!(livekit::StreamType::Downstream, key.stream_type());
        assert_eq!(
            StatsKey {
                country: "NL".to_string(),
                stream_type: livekit::StreamType::Downstream,
                participant_id: "PA_1".into(),
                track_id: "TR_1".into(),
                track_source: livekit::TrackSource::Camera,
                track_type: livekit::TrackType::Video,
                track: true,
            },
            key
        );

        let key = stats_key_for_data(
            "NL",
            livekit::StreamType::Upstream,
            "PA_1".into(),
            "TR_2".into(),
        );
        assert_eq!(livekit::StreamType::Upstream, key.stream_type());
        assert_eq!(
            StatsKey {
                country: "NL".to_string(),
                stream_type: livekit::StreamType::Upstream,
                participant_id: "PA_1".into(),
                track_id: "TR_2".into(),
                track_source: livekit::TrackSource::Unknown,
                track_type: livekit::TrackType::Audio,
                track: false,
            },
            key
        );
    }
}
