// Ported from LiveKit, pkg/telemetry/events.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The `context.Context` of every method is dropped. The
// methods are those of `TelemetryServiceImpl` (`telemetryService` upstream), which its
// implementation of the interface hands on to (`telemetryservice.rs`). A job of the queue
// gets the service and copies of the messages it was called with, where the original's
// closure keeps the caller's pointers: the events are made of the copies. The webhook
// options of `NotifyEvent` are a slice. `egress.GetEgressNotifyOptions` of the protocol's
// `egress/notify_options.go` (Copyright 2025 LiveKit, Inc., Apache-2.0) is
// `get_egress_notify_options` at the end, since musivum has nothing else of that package.
// `newRoomEvent`, `newParticipantEvent` and `newTrackEvent` take the room, the participant and
// the track as `Option`s, which nil is upstream; `toMinimalRoom` gives one. `timestamppb.Now()`
// is the time of `go::time::now()`.

use std::sync::Arc;

use super::AnalyticsService;
use super::prometheus;
use super::statsworker::ReferenceGuard;
use super::telemetryservice::TelemetryServiceImpl;
use crate::go::time::{self, timestamppb_new};
use crate::protocol::codecs::mime::MimeType;
use crate::protocol::livekit;
use crate::protocol::logger;
use crate::protocol::utils::guid;
use crate::protocol::webhook::{self, NotifyOption};
use crate::{infow, warnw};

impl TelemetryServiceImpl {
    pub(super) fn notify_event(&self, mut event: livekit::WebhookEvent, opts: &[NotifyOption]) {
        let Some(notifier) = &self.notifier else {
            return;
        };

        event.created_at = time::now().unix_secs();
        event.id = guid::new("EV_");

        if let Err(err) = notifier.queue_notify(&event, opts) {
            warnw!(logger::get_logger(), "failed to notify webhook", err, "event" => event.event);
        }
    }

    pub(super) fn room_started(&self, room: &livekit::Room) {
        let room = room.clone();
        self.enqueue(move |t| {
            t.notify_event(
                livekit::WebhookEvent {
                    event: webhook::EVENT_ROOM_STARTED.to_string(),
                    room: Some(room.clone()),
                    ..Default::default()
                },
                &[],
            );

            t.send_event(&livekit::AnalyticsEvent {
                r#type: livekit::AnalyticsEventType::RoomCreated as i32,
                timestamp: Some(musivum_protocol::Timestamp {
                    seconds: room.creation_time,
                    nanos: 0,
                }),
                room: Some(room),
                ..Default::default()
            });
        });
    }

    pub(super) fn room_ended(&self, room: &livekit::Room, reason: livekit::RoomEndReason) {
        let room = room.clone();
        self.enqueue(move |t| {
            t.notify_event(
                livekit::WebhookEvent {
                    event: webhook::EVENT_ROOM_FINISHED.to_string(),
                    room: Some(room.clone()),
                    room_end_reason: reason as i32,
                    ..Default::default()
                },
                &[],
            );

            t.send_event(&livekit::AnalyticsEvent {
                r#type: livekit::AnalyticsEventType::RoomEnded as i32,
                timestamp: Some(timestamppb_new(time::now())),
                room_id: room.sid.clone(),
                room: Some(room),
                room_end_reason: reason as i32,
                ..Default::default()
            });
        });
    }

    pub(super) fn participant_joined(
        &self,
        room: &livekit::Room,
        participant: &livekit::ParticipantInfo,
        client_info: Option<&livekit::ClientInfo>,
        client_meta: Option<&livekit::AnalyticsClientMeta>,
        should_send_event: bool,
        guard: Option<Arc<ReferenceGuard>>,
    ) {
        let (room, participant) = (room.clone(), participant.clone());
        let (client_info, client_meta) = (client_info.cloned(), client_meta.cloned());
        self.enqueue(move |t| {
            let (_, found) = t.get_or_create_worker(
                &livekit::RoomId::from(&room.sid),
                &livekit::RoomName::from(&room.name),
                &livekit::ParticipantId::from(&participant.sid),
                &livekit::ParticipantIdentity::from(&participant.identity),
                guard.as_deref(),
            );
            if !found {
                prometheus::increment_participant_rtc_connected(
                    1,
                    client_info
                        .as_ref()
                        .map(|client_info| client_info.sdk())
                        .unwrap_or_default(),
                );
                prometheus::add_participant();
            }

            if should_send_event {
                let mut ev = new_participant_event(
                    livekit::AnalyticsEventType::ParticipantJoined,
                    Some(&room),
                    Some(&participant),
                );
                ev.client_info = client_info;
                ev.client_meta = client_meta;
                t.send_event(&ev);
            }
        });
    }

    pub(super) fn participant_active(
        &self,
        room: &livekit::Room,
        participant: &livekit::ParticipantInfo,
        client_meta: &livekit::AnalyticsClientMeta,
        is_migration: bool,
        is_warp: bool,
        guard: Option<Arc<ReferenceGuard>>,
    ) {
        let (room, participant, client_meta) =
            (room.clone(), participant.clone(), client_meta.clone());
        self.enqueue(move |t| {
            if !is_migration {
                // a participant is considered "joined" only when they become "active"
                t.notify_event(
                    livekit::WebhookEvent {
                        event: webhook::EVENT_PARTICIPANT_JOINED.to_string(),
                        room: Some(room.clone()),
                        participant: Some(participant.clone()),
                        ..Default::default()
                    },
                    &[],
                );
            }

            let (worker, found) = t.get_or_create_worker(
                &livekit::RoomId::from(&room.sid),
                &livekit::RoomName::from(&room.name),
                &livekit::ParticipantId::from(&participant.sid),
                &livekit::ParticipantIdentity::from(&participant.identity),
                guard.as_deref(),
            );
            if !found {
                prometheus::add_participant();
            }
            // SetConnected of a nil worker does nothing
            if let Some(worker) = &worker {
                worker.set_connected();
            }
            prometheus::increment_participant_rtc_active(1, is_warp);

            let mut ev = new_participant_event(
                livekit::AnalyticsEventType::ParticipantActive,
                Some(&room),
                Some(&participant),
            );
            ev.client_meta = Some(client_meta);
            t.send_event(&ev);
        });
    }

    pub(super) fn participant_resumed(
        &self,
        room: &livekit::Room,
        participant: &livekit::ParticipantInfo,
        node_id: &livekit::NodeId,
        reason: livekit::ReconnectReason,
    ) {
        let (room, participant, node_id) = (room.clone(), participant.clone(), node_id.clone());
        self.enqueue(move |t| {
            // create a worker if needed.
            //
            // Signalling channel stats collector and media channel stats collector could both call
            // ParticipantJoined and ParticipantLeft.
            //
            // On a resume, the signalling channel collector would call `ParticipantLeft` which would close
            // the corresponding participant's stats worker.
            //
            // So, on a successful resume, create the worker if needed.
            let (_, found) = t.get_or_create_worker(
                &livekit::RoomId::from(&room.sid),
                &livekit::RoomName::from(&room.name),
                &livekit::ParticipantId::from(&participant.sid),
                &livekit::ParticipantIdentity::from(&participant.identity),
                None,
            );
            if !found {
                prometheus::add_participant();
            }

            let mut ev = new_participant_event(
                livekit::AnalyticsEventType::ParticipantResumed,
                Some(&room),
                Some(&participant),
            );
            ev.client_meta = Some(livekit::AnalyticsClientMeta {
                node: node_id.to_string(),
                reconnect_reason: reason as i32,
                ..Default::default()
            });
            t.send_event(&ev);
        });
    }

    pub(super) fn participant_left(
        &self,
        room: &livekit::Room,
        participant: &livekit::ParticipantInfo,
        should_send_event: bool,
        guard: Option<Arc<ReferenceGuard>>,
    ) {
        let (room, participant) = (room.clone(), participant.clone());
        self.enqueue(move |t| {
            let mut is_connected = false;
            if let Some(worker) = t.get_worker(
                &livekit::RoomId::from(&room.sid),
                &livekit::ParticipantId::from(&participant.sid),
            ) {
                is_connected = worker.is_connected();
                if worker.close(guard.as_deref()) {
                    prometheus::sub_participant();
                } else {
                    infow!(
                        logger::get_logger(),
                        "stats worker active",
                        "room" => room.name,
                        "roomID" => room.sid,
                        "participant" => participant.identity,
                        "participantID" => participant.sid,
                        "worker" => worker,
                        "guard" => guard,
                    );
                }
            }

            if should_send_event {
                let mut webhook_event = webhook::EVENT_PARTICIPANT_LEFT;
                let mut analytics_event = livekit::AnalyticsEventType::ParticipantLeft;
                if !is_connected {
                    webhook_event = webhook::EVENT_PARTICIPANT_CONNECTION_ABORTED;
                    analytics_event = livekit::AnalyticsEventType::ParticipantConnectionAborted;
                }
                t.notify_event(
                    livekit::WebhookEvent {
                        event: webhook_event.to_string(),
                        room: Some(room.clone()),
                        participant: Some(participant.clone()),
                        ..Default::default()
                    },
                    &[],
                );

                t.send_event(&new_participant_event(
                    analytics_event,
                    Some(&room),
                    Some(&participant),
                ));
            }
        });
    }

    pub(super) fn track_publish_requested(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        track: &livekit::TrackInfo,
        should_send_event: bool,
    ) {
        let (room, participant_id, identity, track) = (
            room.clone(),
            participant_id.clone(),
            identity.clone(),
            track.clone(),
        );
        self.enqueue(move |t| {
            prometheus::record_track_publish_attempt(track.r#type().as_str_name());
            if !should_send_event {
                return;
            }

            let mut ev = new_track_event(
                livekit::AnalyticsEventType::TrackPublishRequested,
                Some(&room),
                &participant_id,
                Some(&track),
            );
            if let Some(participant) = &mut ev.participant {
                participant.identity = identity.to_string();
            }
            t.send_event(&ev);
        });
    }

    pub(super) fn track_published(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        track: &livekit::TrackInfo,
        should_send_event: bool,
    ) {
        let (room, participant_id, identity, track) = (
            room.clone(),
            participant_id.clone(),
            identity.clone(),
            track.clone(),
        );
        self.enqueue(move |t| {
            prometheus::add_published_track(track.r#type().as_str_name());
            prometheus::add_packet_trailer_track(&track);
            prometheus::record_track_publish_success(track.r#type().as_str_name());
            if !should_send_event {
                return;
            }

            let participant = livekit::ParticipantInfo {
                sid: participant_id.to_string(),
                identity: identity.to_string(),
                ..Default::default()
            };
            t.notify_event(
                livekit::WebhookEvent {
                    event: webhook::EVENT_TRACK_PUBLISHED.to_string(),
                    room: Some(room.clone()),
                    participant: Some(participant.clone()),
                    track: Some(track.clone()),
                    ..Default::default()
                },
                &[],
            );

            let mut ev = new_track_event(
                livekit::AnalyticsEventType::TrackPublished,
                Some(&room),
                &participant_id,
                Some(&track),
            );
            ev.participant = Some(participant);
            t.send_event(&ev);
        });
    }

    pub(super) fn track_published_update(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
    ) {
        let (room, participant_id, track) = (room.clone(), participant_id.clone(), track.clone());
        self.enqueue(move |t| {
            t.send_event(&new_track_event(
                livekit::AnalyticsEventType::TrackPublishedUpdate,
                Some(&room),
                &participant_id,
                Some(&track),
            ));
        });
    }

    pub(super) fn track_max_subscribed_video_quality(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
        mime: MimeType,
        max_quality: livekit::VideoQuality,
    ) {
        let (room, participant_id, track) = (room.clone(), participant_id.clone(), track.clone());
        self.enqueue(move |t| {
            let mut ev = new_track_event(
                livekit::AnalyticsEventType::TrackMaxSubscribedVideoQuality,
                Some(&room),
                &participant_id,
                Some(&track),
            );
            ev.max_subscribed_video_quality = max_quality as i32;
            ev.mime = mime.to_string();
            t.send_event(&ev);
        });
    }

    pub(super) fn track_subscribe_requested(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
    ) {
        let (room, participant_id, track) = (room.clone(), participant_id.clone(), track.clone());
        self.enqueue(move |t| {
            prometheus::record_track_subscribe_attempt();

            let ev = new_track_event(
                livekit::AnalyticsEventType::TrackSubscribeRequested,
                Some(&room),
                &participant_id,
                Some(&track),
            );
            t.send_event(&ev);
        });
    }

    pub(super) fn track_subscribed(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
        publisher: &livekit::ParticipantInfo,
        should_send_event: bool,
    ) {
        let (room, participant_id, track, publisher) = (
            room.clone(),
            participant_id.clone(),
            track.clone(),
            publisher.clone(),
        );
        self.enqueue(move |t| {
            prometheus::record_track_subscribe_success(track.r#type().as_str_name());

            if !should_send_event {
                return;
            }

            let mut ev = new_track_event(
                livekit::AnalyticsEventType::TrackSubscribed,
                Some(&room),
                &participant_id,
                Some(&track),
            );
            ev.publisher = Some(publisher);
            t.send_event(&ev);
        });
    }

    pub(super) fn track_subscribe_failed(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        err: &str,
        is_user_error: bool,
    ) {
        let (room, participant_id, track_id, err) = (
            room.clone(),
            participant_id.clone(),
            track_id.clone(),
            err.to_string(),
        );
        self.enqueue(move |t| {
            prometheus::record_track_subscribe_failure(&err, is_user_error);

            let mut ev = new_track_event(
                livekit::AnalyticsEventType::TrackSubscribeFailed,
                Some(&room),
                &participant_id,
                Some(&livekit::TrackInfo {
                    sid: track_id.to_string(),
                    ..Default::default()
                }),
            );
            ev.error = err;
            t.send_event(&ev);
        });
    }

    pub(super) fn track_unsubscribed(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
        should_send_event: bool,
    ) {
        let (room, participant_id, track) = (room.clone(), participant_id.clone(), track.clone());
        self.enqueue(move |t| {
            prometheus::record_track_unsubscribed(track.r#type().as_str_name());

            if should_send_event {
                t.send_event(&new_track_event(
                    livekit::AnalyticsEventType::TrackUnsubscribed,
                    Some(&room),
                    &participant_id,
                    Some(&track),
                ));
            }
        });
    }

    pub(super) fn track_unpublished(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        track: &livekit::TrackInfo,
        was_published_locally: bool,
        should_send_event: bool,
    ) {
        let (room, participant_id, identity, track) = (
            room.clone(),
            participant_id.clone(),
            identity.clone(),
            track.clone(),
        );
        self.enqueue(move |t| {
            if was_published_locally {
                prometheus::sub_published_track(track.r#type().as_str_name());
                prometheus::sub_packet_trailer_track(&track);
            }
            if !should_send_event {
                return;
            }

            let participant = livekit::ParticipantInfo {
                sid: participant_id.to_string(),
                identity: identity.to_string(),
                ..Default::default()
            };
            t.notify_event(
                livekit::WebhookEvent {
                    event: webhook::EVENT_TRACK_UNPUBLISHED.to_string(),
                    room: Some(room.clone()),
                    participant: Some(participant),
                    track: Some(track.clone()),
                    ..Default::default()
                },
                &[],
            );

            t.send_event(&new_track_event(
                livekit::AnalyticsEventType::TrackUnpublished,
                Some(&room),
                &participant_id,
                Some(&track),
            ));
        });
    }

    pub(super) fn track_muted(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
    ) {
        let (room, participant_id, track) = (room.clone(), participant_id.clone(), track.clone());
        self.enqueue(move |t| {
            t.send_event(&new_track_event(
                livekit::AnalyticsEventType::TrackMuted,
                Some(&room),
                &participant_id,
                Some(&track),
            ));
        });
    }

    pub(super) fn track_unmuted(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
    ) {
        let (room, participant_id, track) = (room.clone(), participant_id.clone(), track.clone());
        self.enqueue(move |t| {
            t.send_event(&new_track_event(
                livekit::AnalyticsEventType::TrackUnmuted,
                Some(&room),
                &participant_id,
                Some(&track),
            ));
        });
    }

    pub(super) fn track_publish_rtp_stats(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        mime_type: MimeType,
        layer: i32,
        stats: &livekit::RtpStats,
    ) {
        let (room, participant_id, track_id, stats) = (
            room.clone(),
            participant_id.clone(),
            track_id.clone(),
            stats.clone(),
        );
        self.enqueue(move |t| {
            let mut ev = new_room_event(
                livekit::AnalyticsEventType::TrackPublishStats,
                to_minimal_room(Some(&room)),
            );
            ev.participant_id = participant_id.to_string();
            ev.track_id = track_id.to_string();
            ev.mime = mime_type.to_string();
            ev.video_layer = layer;
            ev.rtp_stats = Some(stats);
            t.send_event(&ev);
        });
    }

    pub(super) fn track_subscribe_rtp_stats(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        mime_type: MimeType,
        stats: &livekit::RtpStats,
    ) {
        let (room, participant_id, track_id, stats) = (
            room.clone(),
            participant_id.clone(),
            track_id.clone(),
            stats.clone(),
        );
        self.enqueue(move |t| {
            let mut ev = new_room_event(
                livekit::AnalyticsEventType::TrackSubscribeStats,
                to_minimal_room(Some(&room)),
            );
            ev.participant_id = participant_id.to_string();
            ev.track_id = track_id.to_string();
            ev.mime = mime_type.to_string();
            ev.rtp_stats = Some(stats);
            t.send_event(&ev);
        });
    }

    pub(super) fn notify_egress_event(&self, event: &str, info: &livekit::EgressInfo) {
        let opts = get_egress_notify_options(Some(info));

        self.notify_event(
            livekit::WebhookEvent {
                event: event.to_string(),
                room: Some(to_minimal_room_proto(
                    &livekit::RoomId::from(&info.room_id),
                    &livekit::RoomName::from(&info.room_name),
                )),
                egress_info: Some(info.clone()),
                ..Default::default()
            },
            &opts,
        );
    }

    pub(super) fn egress_started(&self, info: &livekit::EgressInfo) {
        let info = info.clone();
        self.enqueue(move |t| {
            t.notify_egress_event(webhook::EVENT_EGRESS_STARTED, &info);

            t.send_event(&new_egress_event(
                livekit::AnalyticsEventType::EgressStarted,
                &info,
            ));
        });
    }

    pub(super) fn egress_updated(&self, info: &livekit::EgressInfo) {
        let info = info.clone();
        self.enqueue(move |t| {
            t.notify_egress_event(webhook::EVENT_EGRESS_UPDATED, &info);

            t.send_event(&new_egress_event(
                livekit::AnalyticsEventType::EgressUpdated,
                &info,
            ));
        });
    }

    pub(super) fn egress_ended(&self, info: &livekit::EgressInfo) {
        let info = info.clone();
        self.enqueue(move |t| {
            t.notify_egress_event(webhook::EVENT_EGRESS_ENDED, &info);

            t.send_event(&new_egress_event(
                livekit::AnalyticsEventType::EgressEnded,
                &info,
            ));
        });
    }

    pub(super) fn ingress_created(&self, info: &livekit::IngressInfo) {
        let info = info.clone();
        self.enqueue(move |t| {
            t.send_event(&new_ingress_event(
                livekit::AnalyticsEventType::IngressCreated,
                &info,
            ));
        });
    }

    pub(super) fn ingress_deleted(&self, info: &livekit::IngressInfo) {
        let info = info.clone();
        self.enqueue(move |t| {
            t.send_event(&new_ingress_event(
                livekit::AnalyticsEventType::IngressDeleted,
                &info,
            ));
        });
    }

    pub(super) fn ingress_started(&self, info: &livekit::IngressInfo) {
        let info = info.clone();
        self.enqueue(move |t| {
            t.notify_event(
                livekit::WebhookEvent {
                    event: webhook::EVENT_INGRESS_STARTED.to_string(),
                    room: Some(to_minimal_room_proto(
                        &livekit::RoomId::default(),
                        &livekit::RoomName::from(&info.room_name),
                    )),
                    ingress_info: Some(info.clone()),
                    ..Default::default()
                },
                &[],
            );

            t.send_event(&new_ingress_event(
                livekit::AnalyticsEventType::IngressStarted,
                &info,
            ));
        });
    }

    pub(super) fn ingress_updated(&self, info: &livekit::IngressInfo) {
        let info = info.clone();
        self.enqueue(move |t| {
            t.send_event(&new_ingress_event(
                livekit::AnalyticsEventType::IngressUpdated,
                &info,
            ));
        });
    }

    pub(super) fn ingress_ended(&self, info: &livekit::IngressInfo) {
        let info = info.clone();
        self.enqueue(move |t| {
            t.notify_event(
                livekit::WebhookEvent {
                    event: webhook::EVENT_INGRESS_ENDED.to_string(),
                    room: Some(to_minimal_room_proto(
                        &livekit::RoomId::default(),
                        &livekit::RoomName::from(&info.room_name),
                    )),
                    ingress_info: Some(info.clone()),
                    ..Default::default()
                },
                &[],
            );

            t.send_event(&new_ingress_event(
                livekit::AnalyticsEventType::IngressEnded,
                &info,
            ));
        });
    }

    pub(super) fn report(&self, report_info: &livekit::ReportInfo) {
        let report_info = report_info.clone();
        self.enqueue(move |t| {
            let ev = livekit::AnalyticsEvent {
                r#type: livekit::AnalyticsEventType::Report as i32,
                timestamp: Some(timestamppb_new(time::now())),
                report: Some(report_info),
                ..Default::default()
            };
            t.send_event(&ev);
        });
    }

    pub(super) fn api_call(&self, api_call_info: &livekit::ApiCallInfo) {
        let api_call_info = api_call_info.clone();
        self.enqueue(move |t| {
            let ev = livekit::AnalyticsEvent {
                r#type: livekit::AnalyticsEventType::ApiCall as i32,
                timestamp: Some(timestamppb_new(time::now())),
                api_call: Some(api_call_info),
                ..Default::default()
            };
            t.send_event(&ev);
        });
    }

    pub(super) fn webhook(&self, webhook_info: &livekit::WebhookInfo) {
        let webhook_info = webhook_info.clone();
        self.enqueue(move |t| {
            let ev = livekit::AnalyticsEvent {
                r#type: livekit::AnalyticsEventType::Webhook as i32,
                timestamp: Some(timestamppb_new(time::now())),
                webhook: Some(webhook_info),
                ..Default::default()
            };
            t.send_event(&ev);
        });
    }
}

fn new_room_event(
    event: livekit::AnalyticsEventType,
    room: Option<livekit::Room>,
) -> livekit::AnalyticsEvent {
    let mut ev = livekit::AnalyticsEvent {
        r#type: event as i32,
        timestamp: Some(timestamppb_new(time::now())),
        ..Default::default()
    };
    if let Some(room) = room {
        ev.room_id = room.sid.clone();
        ev.room = Some(room);
    }
    ev
}

fn new_participant_event(
    event: livekit::AnalyticsEventType,
    room: Option<&livekit::Room>,
    participant: Option<&livekit::ParticipantInfo>,
) -> livekit::AnalyticsEvent {
    let mut ev = new_room_event(event, room.cloned());
    if let Some(participant) = participant {
        ev.participant_id = participant.sid.clone();
        ev.participant = Some(participant.clone());
    }
    ev
}

fn new_track_event(
    event: livekit::AnalyticsEventType,
    room: Option<&livekit::Room>,
    participant_id: &livekit::ParticipantId,
    track: Option<&livekit::TrackInfo>,
) -> livekit::AnalyticsEvent {
    let mut ev = new_participant_event(
        event,
        to_minimal_room(room).as_ref(),
        Some(&livekit::ParticipantInfo {
            sid: participant_id.to_string(),
            ..Default::default()
        }),
    );
    if let Some(track) = track {
        ev.track_id = track.sid.clone();
        ev.track = Some(track.clone());
    }
    ev
}

fn new_egress_event(
    event: livekit::AnalyticsEventType,
    egress: &livekit::EgressInfo,
) -> livekit::AnalyticsEvent {
    livekit::AnalyticsEvent {
        r#type: event as i32,
        timestamp: Some(timestamppb_new(time::now())),
        egress_id: egress.egress_id.clone(),
        room_id: egress.room_id.clone(),
        egress: Some(egress.clone()),
        ..Default::default()
    }
}

fn new_ingress_event(
    event: livekit::AnalyticsEventType,
    ingress: &livekit::IngressInfo,
) -> livekit::AnalyticsEvent {
    livekit::AnalyticsEvent {
        r#type: event as i32,
        timestamp: Some(timestamppb_new(time::now())),
        ingress_id: ingress.ingress_id.clone(),
        ingress: Some(ingress.clone()),
        ..Default::default()
    }
}

fn to_minimal_room(room: Option<&livekit::Room>) -> Option<livekit::Room> {
    let room = room?;

    Some(to_minimal_room_proto(
        &livekit::RoomId::from(&room.sid),
        &livekit::RoomName::from(&room.name),
    ))
}

fn to_minimal_room_proto(
    room_id: &livekit::RoomId,
    room_name: &livekit::RoomName,
) -> livekit::Room {
    livekit::Room {
        sid: room_id.to_string(),
        name: room_name.to_string(),
        ..Default::default()
    }
}

// GetEgressNotifyOptions of the protocol's egress/notify_options.go: see the header.
fn get_egress_notify_options(egress_info: Option<&livekit::EgressInfo>) -> Vec<NotifyOption> {
    let Some(egress_info) = egress_info else {
        return Vec::new();
    };

    let Some(request) = &egress_info.request else {
        return Vec::new();
    };

    use livekit::egress_info::Request;
    let whs: &[livekit::WebhookConfig] = match request {
        Request::Egress(req) => &req.webhooks,
        Request::Replay(req) => &req.webhooks,
        Request::RoomComposite(req) => &req.webhooks,
        Request::Web(req) => &req.webhooks,
        Request::Participant(req) => &req.webhooks,
        Request::TrackComposite(req) => &req.webhooks,
        Request::Track(req) => &req.webhooks,
    };

    if !whs.is_empty() {
        return vec![webhook::with_extra_webhooks(whs.to_vec())];
    }

    Vec::new()
}

#[cfg(test)]
#[path = "events_test.rs"]
mod events_test;
