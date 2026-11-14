// Ported from LiveKit, pkg/telemetry/stats.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only `StatsKey` with its constructors is here:
// `TrackStats` of `telemetryService`, which counts a track's statistics into the Prometheus
// collectors and hands them to the participant's stats worker, is of the implementation of
// the service, which is not ported. It is the one reader of the key's fields, which are
// visible in this module for what takes its place. The key can be compared, as a Go struct
// can. The test at the end is new: the original has none of the key by itself.

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
