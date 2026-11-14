// Ported from LiveKit, pkg/telemetry/telemetryservice.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only the interface and `NullTelemetryService` are
// here: `telemetryService`, which turns the calls into analytics events, statistics and
// webhooks (with `events.go`, the stats workers and the queue of jobs), is not ported; what
// musivum needs of it is written new behind this trait. The `context.Context` that is the
// first parameter of the event methods is dropped from every method: musivum carries no
// tracing context (`pkg/utils/context.go` is not ported). A message of the protocol is
// borrowed, and so are an identifier and the stats key; `client_info` and `client_meta` of
// `participant_joined`, which upstream is handed nil for, are `Option`s. A guard is an
// `Option<Arc<ReferenceGuard>>`: the service keeps it, and nil has a meaning there. The
// `error` of `track_subscribe_failed` is its text, which is all upstream uses of it and what
// a fake can record. The `layer int` of `track_publish_rtp_stats` is an `i32`, as the layers
// of `pkg/sfu` are. The embedded `AnalyticsService` is a supertrait, and
// `NullTelemetryService`, which embeds `NullAnalyticService`, has that trait's methods
// written out. The tests at the end are new: the original has none of the interface.

use std::sync::Arc;

use super::analyticsservice::{AnalyticsService, NullAnalyticService};
use super::stats::StatsKey;
use super::statsworker::ReferenceGuard;
use crate::protocol::codecs::mime::MimeType;
use crate::protocol::livekit;

#[cfg_attr(test, musivum_fakes::fake)]
pub trait TelemetryService: AnalyticsService {
    // TrackStats is called periodically for each track in both directions (published/subscribed)
    fn track_stats(
        &self,
        room_id: &livekit::RoomId,
        room_name: &livekit::RoomName,
        key: &StatsKey,
        stat: &livekit::AnalyticsStat,
    );

    // events
    fn room_started(&self, room: &livekit::Room);
    fn room_ended(&self, room: &livekit::Room, reason: livekit::RoomEndReason);

    // ParticipantJoined - a participant establishes signal connection to a room
    fn participant_joined(
        &self,
        room: &livekit::Room,
        participant: &livekit::ParticipantInfo,
        client_info: Option<&livekit::ClientInfo>,
        client_meta: Option<&livekit::AnalyticsClientMeta>,
        should_send_event: bool,
        guard: Option<Arc<ReferenceGuard>>,
    );
    // ParticipantActive - a participant establishes media connection
    fn participant_active(
        &self,
        room: &livekit::Room,
        participant: &livekit::ParticipantInfo,
        client_meta: &livekit::AnalyticsClientMeta,
        is_migration: bool,
        is_warp: bool,
        guard: Option<Arc<ReferenceGuard>>,
    );
    // ParticipantResumed - there has been an ICE restart or connection resume attempt, and we've received their signal connection
    fn participant_resumed(
        &self,
        room: &livekit::Room,
        participant: &livekit::ParticipantInfo,
        node_id: &livekit::NodeId,
        reason: livekit::ReconnectReason,
    );
    // ParticipantLeft - the participant leaves the room, only sent if ParticipantActive has been called before
    fn participant_left(
        &self,
        room: &livekit::Room,
        participant: &livekit::ParticipantInfo,
        should_send_event: bool,
        guard: Option<Arc<ReferenceGuard>>,
    );
    // TrackPublishRequested - a publication attempt has been received
    fn track_publish_requested(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        track: &livekit::TrackInfo,
        should_send_event: bool,
    );
    // TrackPublished - a publication attempt has been successful
    fn track_published(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        track: &livekit::TrackInfo,
        should_send_event: bool,
    );
    // TrackUnpublished - a participant unpublished a track
    fn track_unpublished(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        track: &livekit::TrackInfo,
        was_published_locally: bool,
        should_send_event: bool,
    );
    // TrackSubscribeRequested - a participant requested to subscribe to a track
    fn track_subscribe_requested(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
    );
    // TrackSubscribed - a participant subscribed to a track successfully
    fn track_subscribed(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
        publisher: &livekit::ParticipantInfo,
        should_send_event: bool,
    );
    // TrackUnsubscribed - a participant unsubscribed from a track successfully
    fn track_unsubscribed(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
        should_send_event: bool,
    );
    // TrackSubscribeFailed - failure to subscribe to a track
    fn track_subscribe_failed(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        err: &str,
        is_user_error: bool,
    );
    // TrackMuted - the publisher has muted the Track
    fn track_muted(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
    );
    // TrackUnmuted - the publisher has muted the Track
    fn track_unmuted(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
    );
    // TrackPublishedUpdate - track metadata has been updated
    fn track_published_update(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
    );
    // TrackMaxSubscribedVideoQuality - publisher is notified of the max quality subscribers desire
    fn track_max_subscribed_video_quality(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
        mime: MimeType,
        max_quality: livekit::VideoQuality,
    );
    fn track_publish_rtp_stats(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        mime_type: MimeType,
        layer: i32,
        stats: &livekit::RtpStats,
    );
    fn track_subscribe_rtp_stats(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        mime_type: MimeType,
        stats: &livekit::RtpStats,
    );

    fn egress_started(&self, info: &livekit::EgressInfo);
    fn egress_updated(&self, info: &livekit::EgressInfo);
    fn egress_ended(&self, info: &livekit::EgressInfo);

    fn ingress_created(&self, info: &livekit::IngressInfo);
    fn ingress_deleted(&self, info: &livekit::IngressInfo);
    fn ingress_started(&self, info: &livekit::IngressInfo);
    fn ingress_updated(&self, info: &livekit::IngressInfo);
    fn ingress_ended(&self, info: &livekit::IngressInfo);

    fn local_room_state(&self, info: &livekit::AnalyticsNodeRooms);

    fn report(&self, report_info: &livekit::ReportInfo);

    fn api_call(&self, api_call_info: &livekit::ApiCallInfo);

    fn webhook(&self, webhook_info: &livekit::WebhookInfo);

    // helpers
    // AnalyticsService: the supertrait
    fn notify_egress_event(&self, event: &str, info: &livekit::EgressInfo);
    fn flush_stats(&self);
}

// -----------------------------

#[derive(Clone, Copy, Debug, Default)]
pub struct NullTelemetryService {
    pub null_analytic_service: NullAnalyticService,
}

// The methods of the embedded `NullAnalyticService`.
impl AnalyticsService for NullTelemetryService {
    fn send_stats(&self, stats: &[livekit::AnalyticsStat]) {
        self.null_analytic_service.send_stats(stats);
    }
    fn send_event(&self, events: &livekit::AnalyticsEvent) {
        self.null_analytic_service.send_event(events);
    }
    fn send_node_room_states(&self, node_rooms: &livekit::AnalyticsNodeRooms) {
        self.null_analytic_service.send_node_room_states(node_rooms);
    }
}

impl TelemetryService for NullTelemetryService {
    fn track_stats(
        &self,
        _room_id: &livekit::RoomId,
        _room_name: &livekit::RoomName,
        _key: &StatsKey,
        _stat: &livekit::AnalyticsStat,
    ) {
    }
    fn room_started(&self, _room: &livekit::Room) {}
    fn room_ended(&self, _room: &livekit::Room, _reason: livekit::RoomEndReason) {}
    fn participant_joined(
        &self,
        _room: &livekit::Room,
        _participant: &livekit::ParticipantInfo,
        _client_info: Option<&livekit::ClientInfo>,
        _client_meta: Option<&livekit::AnalyticsClientMeta>,
        _should_send_event: bool,
        _guard: Option<Arc<ReferenceGuard>>,
    ) {
    }
    fn participant_active(
        &self,
        _room: &livekit::Room,
        _participant: &livekit::ParticipantInfo,
        _client_meta: &livekit::AnalyticsClientMeta,
        _is_migration: bool,
        _is_warp: bool,
        _guard: Option<Arc<ReferenceGuard>>,
    ) {
    }
    fn participant_resumed(
        &self,
        _room: &livekit::Room,
        _participant: &livekit::ParticipantInfo,
        _node_id: &livekit::NodeId,
        _reason: livekit::ReconnectReason,
    ) {
    }
    fn participant_left(
        &self,
        _room: &livekit::Room,
        _participant: &livekit::ParticipantInfo,
        _should_send_event: bool,
        _guard: Option<Arc<ReferenceGuard>>,
    ) {
    }
    fn track_publish_requested(
        &self,
        _room: &livekit::Room,
        _participant_id: &livekit::ParticipantId,
        _identity: &livekit::ParticipantIdentity,
        _track: &livekit::TrackInfo,
        _should_send_event: bool,
    ) {
    }
    fn track_published(
        &self,
        _room: &livekit::Room,
        _participant_id: &livekit::ParticipantId,
        _identity: &livekit::ParticipantIdentity,
        _track: &livekit::TrackInfo,
        _should_send_event: bool,
    ) {
    }
    fn track_unpublished(
        &self,
        _room: &livekit::Room,
        _participant_id: &livekit::ParticipantId,
        _identity: &livekit::ParticipantIdentity,
        _track: &livekit::TrackInfo,
        _was_published_locally: bool,
        _should_send_event: bool,
    ) {
    }
    fn track_subscribe_requested(
        &self,
        _room: &livekit::Room,
        _participant_id: &livekit::ParticipantId,
        _track: &livekit::TrackInfo,
    ) {
    }
    fn track_subscribed(
        &self,
        _room: &livekit::Room,
        _participant_id: &livekit::ParticipantId,
        _track: &livekit::TrackInfo,
        _publisher: &livekit::ParticipantInfo,
        _should_send_event: bool,
    ) {
    }
    fn track_unsubscribed(
        &self,
        _room: &livekit::Room,
        _participant_id: &livekit::ParticipantId,
        _track: &livekit::TrackInfo,
        _should_send_event: bool,
    ) {
    }
    fn track_subscribe_failed(
        &self,
        _room: &livekit::Room,
        _participant_id: &livekit::ParticipantId,
        _track_id: &livekit::TrackId,
        _err: &str,
        _is_user_error: bool,
    ) {
    }
    fn track_muted(
        &self,
        _room: &livekit::Room,
        _participant_id: &livekit::ParticipantId,
        _track: &livekit::TrackInfo,
    ) {
    }
    fn track_unmuted(
        &self,
        _room: &livekit::Room,
        _participant_id: &livekit::ParticipantId,
        _track: &livekit::TrackInfo,
    ) {
    }
    fn track_published_update(
        &self,
        _room: &livekit::Room,
        _participant_id: &livekit::ParticipantId,
        _track: &livekit::TrackInfo,
    ) {
    }
    fn track_max_subscribed_video_quality(
        &self,
        _room: &livekit::Room,
        _participant_id: &livekit::ParticipantId,
        _track: &livekit::TrackInfo,
        _mime: MimeType,
        _max_quality: livekit::VideoQuality,
    ) {
    }
    fn track_publish_rtp_stats(
        &self,
        _room: &livekit::Room,
        _participant_id: &livekit::ParticipantId,
        _track_id: &livekit::TrackId,
        _mime_type: MimeType,
        _layer: i32,
        _stats: &livekit::RtpStats,
    ) {
    }
    fn track_subscribe_rtp_stats(
        &self,
        _room: &livekit::Room,
        _participant_id: &livekit::ParticipantId,
        _track_id: &livekit::TrackId,
        _mime_type: MimeType,
        _stats: &livekit::RtpStats,
    ) {
    }
    fn egress_started(&self, _info: &livekit::EgressInfo) {}
    fn egress_updated(&self, _info: &livekit::EgressInfo) {}
    fn egress_ended(&self, _info: &livekit::EgressInfo) {}
    fn ingress_created(&self, _info: &livekit::IngressInfo) {}
    fn ingress_deleted(&self, _info: &livekit::IngressInfo) {}
    fn ingress_started(&self, _info: &livekit::IngressInfo) {}
    fn ingress_updated(&self, _info: &livekit::IngressInfo) {}
    fn ingress_ended(&self, _info: &livekit::IngressInfo) {}
    fn local_room_state(&self, _info: &livekit::AnalyticsNodeRooms) {}
    fn report(&self, _report_info: &livekit::ReportInfo) {}
    fn api_call(&self, _api_call_info: &livekit::ApiCallInfo) {}
    fn webhook(&self, _webhook_info: &livekit::WebhookInfo) {}
    fn notify_egress_event(&self, _event: &str, _info: &livekit::EgressInfo) {}
    fn flush_stats(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::stats_key_for_track;

    fn room() -> livekit::Room {
        livekit::Room {
            sid: "RM_1".to_string(),
            name: "a-room".to_string(),
            ..Default::default()
        }
    }

    fn participant() -> livekit::ParticipantInfo {
        livekit::ParticipantInfo {
            sid: "PA_1".to_string(),
            identity: "alice".to_string(),
            ..Default::default()
        }
    }

    // New for musivum: what upstream's tests have of counterfeiter's `FakeTelemetryService`.
    #[test]
    fn the_fake_records_calls_and_gives_their_arguments_back() {
        let fake = Arc::new(FakeTelemetryService::default());
        let telemetry: Arc<dyn TelemetryService> = fake.clone();

        // messages, two that may be absent, and a guard: the fake has the guard itself
        let client_meta = livekit::AnalyticsClientMeta {
            region: "a-region".to_string(),
            ..Default::default()
        };
        let guard = Arc::new(ReferenceGuard::default());
        telemetry.participant_joined(
            &room(),
            &participant(),
            None,
            Some(&client_meta),
            true,
            Some(guard.clone()),
        );
        assert_eq!(1, fake.participant_joined_call_count());
        let (r, p, client_info, meta, should_send_event, g) =
            fake.participant_joined_args_for_call(0);
        assert_eq!(room(), r);
        assert_eq!(participant(), p);
        assert_eq!(None, client_info);
        assert_eq!(Some(client_meta), meta);
        assert!(should_send_event);
        assert!(Arc::ptr_eq(&guard, &g.unwrap()));

        // identifiers, the key and the statistics, twice
        let key = stats_key_for_track(
            "NL",
            livekit::StreamType::Downstream,
            "PA_1".into(),
            "TR_1".into(),
            livekit::TrackSource::Camera,
            livekit::TrackType::Video,
        );
        let stat = |score| livekit::AnalyticsStat {
            score,
            ..Default::default()
        };
        telemetry.track_stats(&"RM_1".into(), &"a-room".into(), &key, &stat(4.5));
        telemetry.track_stats(&"RM_1".into(), &"a-room".into(), &key, &stat(3.5));
        assert_eq!(2, fake.track_stats_call_count());
        assert_eq!(
            (
                livekit::RoomId::from("RM_1"),
                livekit::RoomName::from("a-room"),
                key.clone(),
                stat(3.5)
            ),
            fake.track_stats_args_for_call(1)
        );

        // an error is recorded as its text
        let err = std::io::Error::other("track not found");
        telemetry.track_subscribe_failed(
            &room(),
            &"PA_1".into(),
            &"TR_2".into(),
            &err.to_string(),
            true,
        );
        assert_eq!(
            (
                room(),
                livekit::ParticipantId::from("PA_1"),
                livekit::TrackId::from("TR_2"),
                "track not found".to_string(),
                true
            ),
            fake.track_subscribe_failed_args_for_call(0)
        );

        // a mime type and numbers
        let stats = livekit::RtpStats {
            packets: 10,
            ..Default::default()
        };
        telemetry.track_publish_rtp_stats(
            &room(),
            &"PA_1".into(),
            &"TR_1".into(),
            MimeType::Vp8,
            2,
            &stats,
        );
        let (_, _, track_id, mime_type, layer, s) = fake.track_publish_rtp_stats_args_for_call(0);
        assert_eq!(
            (livekit::TrackId::from("TR_1"), MimeType::Vp8, 2, stats),
            (track_id, mime_type, layer, s)
        );

        // a method of the embedded `AnalyticsService`, and one without arguments
        let event = livekit::AnalyticsEvent {
            room_id: "RM_1".to_string(),
            ..Default::default()
        };
        telemetry.send_event(&event);
        assert_eq!(event, fake.send_event_args_for_call(0));
        telemetry.flush_stats();
        assert_eq!(1, fake.flush_stats_call_count());

        // what was not called
        assert_eq!(0, fake.participant_left_call_count());
        assert_eq!(0, fake.send_stats_call_count());
    }

    // New for musivum.
    #[test]
    fn the_null_service_can_be_called_through_a_trait_object() {
        let telemetry: Arc<dyn TelemetryService> = Arc::new(NullTelemetryService::default());
        let guard = Arc::new(ReferenceGuard::default());

        telemetry.room_started(&room());
        telemetry.participant_joined(
            &room(),
            &participant(),
            None,
            None,
            false,
            Some(guard.clone()),
        );
        telemetry.participant_active(
            &room(),
            &participant(),
            &livekit::AnalyticsClientMeta::default(),
            false,
            false,
            Some(guard.clone()),
        );
        telemetry.track_published(
            &room(),
            &"PA_1".into(),
            &"alice".into(),
            &livekit::TrackInfo::default(),
            true,
        );
        telemetry.participant_left(&room(), &participant(), true, Some(guard.clone()));
        telemetry.room_ended(&room(), livekit::RoomEndReason::RoomEndIdleTimeout);
        telemetry.send_stats(&[livekit::AnalyticsStat::default()]);
        telemetry.flush_stats();

        // nothing uses the guard
        assert_eq!(1, Arc::strong_count(&guard));
        assert!(!guard.activated.load());
        assert!(!guard.released.load());
    }
}
