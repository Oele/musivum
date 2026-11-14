// Ported from LiveKit, pkg/telemetry/telemetryservice.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The `context.Context` that is the first parameter of
// the event methods is dropped from every method: musivum carries no tracing context
// (`pkg/utils/context.go` is not ported). A message of the protocol is borrowed, and so are an
// identifier and the stats key; `client_info` and `client_meta` of `participant_joined`, which
// upstream is handed nil for, are `Option`s. A guard is an `Option<Arc<ReferenceGuard>>`: the
// service keeps it, and nil has a meaning there. The `error` of `track_subscribe_failed` is its
// text, which is all upstream uses of it and what a fake can record. The `layer int` of
// `track_publish_rtp_stats` is an `i32`, as the layers of `pkg/sfu` are. The embedded
// `AnalyticsService` is a supertrait, and `NullTelemetryService`, which embeds
// `NullAnalyticService`, has that trait's methods written out.
// `telemetryService` is `TelemetryServiceImpl` (its name in Rust's spelling is the trait's),
// made by `new_telemetry_service` with the notifier as an `Option` (nil upstream when there is
// none). It implements the trait in one block here, whose methods hand on to the methods of
// the same names that `events.rs`, `stats.rs` and this file have, as upstream has them in
// those files; the embedded `AnalyticsService` is a field the supertrait's methods hand on to.
// It is an `Arc` that knows itself (`weak_self`): a job of the queue keeps it until it has run.
// The queue of jobs is the ported `utils::OpsQueue` (whose worker is a thread, see there); the
// goroutine of `run` is a task with a ticker that skips the ticks it misses, which keeps a
// `Weak` of the service and ends with it (upstream it runs for ever). The list of workers is
// linked through each worker's `next`, which has a lock of its own; the reaped workers are
// unlinked as they are taken off the map. `new_inner` makes the service without starting its
// queue or its ticker, for the test that upstream gives a bare struct. The tests at the end
// are new: the original has none of the interface.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use super::analyticsservice::{AnalyticsService, NullAnalyticService};
use super::stats::StatsKey;
use super::statsworker::{ReferenceGuard, StatsWorker, new_stats_worker};
use crate::go::time::{self, Duration, MINUTE, SECOND};
use crate::protocol::codecs::mime::MimeType;
use crate::protocol::livekit;
use crate::protocol::logger;
use crate::protocol::webhook::QueuedNotifier;
use crate::utils::{OpsQueue, OpsQueueParams};
use crate::{infow, warnw};

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

// -----------------------------

const WORKER_CLEANUP_WAIT: Duration = Duration(3 * MINUTE.0);
const JOBS_QUEUE_MIN_SIZE: usize = 2048;

const TELEMETRY_STATS_UPDATE_INTERVAL: Duration = Duration(30 * SECOND.0);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct StatsWorkerKey {
    room_id: livekit::RoomId,
    participant_id: livekit::ParticipantId,
}

pub(super) struct TelemetryServiceImpl {
    analytics_service: Arc<dyn AnalyticsService>,

    pub(super) notifier: Option<Arc<dyn QueuedNotifier>>,
    jobs_queue: OpsQueue,

    workers_mu: RwLock<TelemetryServiceWorkers>,

    flush_mu: Mutex<()>,

    pub(super) weak_self: Weak<TelemetryServiceImpl>,
}

// What `workersMu` guards.
struct TelemetryServiceWorkers {
    workers: HashMap<StatsWorkerKey, Arc<StatsWorker>>,
    worker_list: Option<Arc<StatsWorker>>,
}

pub fn new_telemetry_service(
    notifier: Option<Arc<dyn QueuedNotifier>>,
    analytics: Arc<dyn AnalyticsService>,
) -> Arc<dyn TelemetryService> {
    let t = TelemetryServiceImpl::new_inner(notifier, analytics);

    t.jobs_queue.start();
    let weak = Arc::downgrade(&t);
    time::spawn(async move { TelemetryServiceImpl::run(weak).await });

    t
}

impl TelemetryServiceImpl {
    // NewTelemetryService without starting the queue and the ticker: see the header.
    pub(super) fn new_inner(
        notifier: Option<Arc<dyn QueuedNotifier>>,
        analytics: Arc<dyn AnalyticsService>,
    ) -> Arc<TelemetryServiceImpl> {
        Arc::new_cyclic(|weak_self| TelemetryServiceImpl {
            analytics_service: analytics,
            notifier,
            jobs_queue: OpsQueue::new(OpsQueueParams {
                name: "telemetry".to_string(),
                min_size: JOBS_QUEUE_MIN_SIZE,
                flush_on_stop: true,
                logger: logger::get_logger(),
            }),
            workers_mu: RwLock::new(TelemetryServiceWorkers {
                workers: HashMap::new(),
                worker_list: None,
            }),
            flush_mu: Mutex::new(()),
            weak_self: weak_self.clone(),
        })
    }

    pub(super) fn flush_stats(&self) {
        let _flush = self.flush_mu.lock();

        let t = self.workers_mu.read();
        let mut worker = t.worker_list.clone();
        drop(t);

        let now = time::now();
        let mut prev: Option<Arc<StatsWorker>> = None;
        let mut reap: Option<Arc<StatsWorker>> = None;
        while let Some(w) = worker {
            let next = w.next.lock().clone();
            let closed = w.flush(now, WORKER_CLEANUP_WAIT);
            if closed {
                let w_next = w.next.lock().clone();
                match &prev {
                    None => {
                        // this worker was at the head of the list
                        let mut t = self.workers_mu.write();
                        if same_worker(&t.worker_list, &w) {
                            t.worker_list = w_next;
                        } else {
                            let mut p = t.worker_list.clone().expect("the worker is in the list");
                            loop {
                                // new workers have been added. scan until we find the one
                                // immediately before this
                                let p_next = p.next.lock().clone();
                                if same_worker(&p_next, &w) {
                                    break;
                                }
                                p = p_next.expect("the worker is in the list");
                            }
                            *p.next.lock() = w_next;
                            prev = Some(p);
                        }
                        drop(t);
                    }
                    Some(prev) => {
                        *prev.next.lock() = w_next;
                    }
                }

                *w.next.lock() = reap.take();
                reap = Some(w);
            } else {
                prev = Some(w);
            }
            worker = next;
        }

        if reap.is_some() {
            let mut t = self.workers_mu.write();
            while let Some(r) = reap {
                let key = StatsWorkerKey {
                    room_id: r.room_id.clone(),
                    participant_id: r.participant_id.clone(),
                };
                if t.workers.get(&key).is_some_and(|w| Arc::ptr_eq(w, &r)) {
                    t.workers.remove(&key);
                }
                // unlinked as it goes: see the header
                reap = r.next.lock().take();
            }
            drop(t);
        }
    }

    async fn run(t: Weak<TelemetryServiceImpl>) {
        let period = TELEMETRY_STATS_UPDATE_INTERVAL.to_std();
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let Some(t) = t.upgrade() else {
                return;
            };
            t.flush_stats();
        }
    }

    pub(super) fn enqueue(&self, op: impl FnOnce(Arc<TelemetryServiceImpl>) + Send + 'static) {
        let Some(t) = self.weak_self.upgrade() else {
            return;
        };
        self.jobs_queue.enqueue(move || op(t));
    }

    pub(super) fn get_worker(
        &self,
        room_id: &livekit::RoomId,
        participant_id: &livekit::ParticipantId,
    ) -> Option<Arc<StatsWorker>> {
        let t = self.workers_mu.read();

        t.workers
            .get(&StatsWorkerKey {
                room_id: room_id.clone(),
                participant_id: participant_id.clone(),
            })
            .cloned()
    }

    pub(super) fn get_or_create_worker(
        &self,
        room_id: &livekit::RoomId,
        room_name: &livekit::RoomName,
        participant_id: &livekit::ParticipantId,
        participant_identity: &livekit::ParticipantIdentity,
        guard: Option<&ReferenceGuard>,
    ) -> (Option<Arc<StatsWorker>>, bool) {
        let mut t = self.workers_mu.write();

        if room_id.is_empty() {
            warnw!(
                logger::get_logger(),
                "telemetry stats worker keyed under an empty room id",
                nil,
                "room" => room_name,
                "participant" => participant_identity,
                "participantID" => participant_id,
                "guard" => guard,
            );
        }

        let key = StatsWorkerKey {
            room_id: room_id.clone(),
            participant_id: participant_id.clone(),
        };
        let worker = t.workers.get(&key).cloned();
        let ok = worker.is_some();
        if let Some(w) = &worker
            && !w.closed(guard)
        {
            return (worker, true);
        }

        // only ParticipantLeft releases a guard, so a released guard is a call landing after
        // the participant left, e.g. ParticipantActive overtaken by the close. Do not create
        // a worker nothing can ever release. The closed worker, if not yet reaped, is returned
        // as found, otherwise nil is
        if let Some(guard) = guard
            && guard.released.load()
        {
            return (worker, true);
        }

        let mut existing_is_connected = false;
        if let Some(w) = &worker {
            existing_is_connected = w.is_connected();
        }

        // a guard references at most once, so a nil or already activated guard leaves the
        // new worker with no references and its owner's release drives it negative
        if guard.is_none_or(|guard| guard.activated.load()) {
            infow!(
                logger::get_logger(),
                "telemetry stats worker created without a reference",
                "room" => room_name,
                "roomID" => room_id,
                "participant" => participant_identity,
                "participantID" => participant_id,
                "guard" => guard,
                "replacedClosed" => ok,
                "existing" => worker,
            );
        }

        let t_service: Weak<dyn TelemetryService> = self.weak_self.clone();
        let worker = new_stats_worker(
            t_service,
            room_id.clone(),
            room_name.clone(),
            participant_id.clone(),
            participant_identity.clone(),
            guard,
        );
        if existing_is_connected {
            worker.set_connected();
        }

        t.workers.insert(key, worker.clone());

        *worker.next.lock() = t.worker_list.take();
        t.worker_list = Some(worker.clone());

        (Some(worker), false)
    }

    pub(super) fn local_room_state(&self, info: &livekit::AnalyticsNodeRooms) {
        let info = info.clone();
        self.enqueue(move |t| {
            t.send_node_room_states(&info);
        });
    }

    // For the tests: the map of workers.
    #[cfg(test)]
    pub(super) fn worker_in_map(
        &self,
        room_id: &livekit::RoomId,
        participant_id: &livekit::ParticipantId,
    ) -> Option<Arc<StatsWorker>> {
        self.get_worker(room_id, participant_id)
    }

    #[cfg(test)]
    pub(super) fn remove_from_map(
        &self,
        room_id: &livekit::RoomId,
        participant_id: &livekit::ParticipantId,
    ) {
        self.workers_mu.write().workers.remove(&StatsWorkerKey {
            room_id: room_id.clone(),
            participant_id: participant_id.clone(),
        });
    }

    #[cfg(test)]
    pub(super) fn num_workers_in_map(&self) -> usize {
        self.workers_mu.read().workers.len()
    }
}

fn same_worker(a: &Option<Arc<StatsWorker>>, b: &Arc<StatsWorker>) -> bool {
    a.as_ref().is_some_and(|a| Arc::ptr_eq(a, b))
}

// New: the list of workers is unlinked one by one, not by recursion.
impl Drop for TelemetryServiceImpl {
    fn drop(&mut self) {
        let mut worker = self.workers_mu.get_mut().worker_list.take();
        while let Some(w) = worker {
            worker = w.next.lock().take();
        }
    }
}

// The embedded AnalyticsService.
impl AnalyticsService for TelemetryServiceImpl {
    fn send_stats(&self, stats: &[livekit::AnalyticsStat]) {
        self.analytics_service.send_stats(stats);
    }
    fn send_event(&self, events: &livekit::AnalyticsEvent) {
        self.analytics_service.send_event(events);
    }
    fn send_node_room_states(&self, node_rooms: &livekit::AnalyticsNodeRooms) {
        self.analytics_service.send_node_room_states(node_rooms);
    }
}

// The methods of the interface, in the files upstream has them in: see the header.
impl TelemetryService for TelemetryServiceImpl {
    fn track_stats(
        &self,
        room_id: &livekit::RoomId,
        room_name: &livekit::RoomName,
        key: &StatsKey,
        stat: &livekit::AnalyticsStat,
    ) {
        TelemetryServiceImpl::track_stats(self, room_id, room_name, key, stat);
    }
    fn room_started(&self, room: &livekit::Room) {
        TelemetryServiceImpl::room_started(self, room);
    }
    fn room_ended(&self, room: &livekit::Room, reason: livekit::RoomEndReason) {
        TelemetryServiceImpl::room_ended(self, room, reason);
    }
    fn participant_joined(
        &self,
        room: &livekit::Room,
        participant: &livekit::ParticipantInfo,
        client_info: Option<&livekit::ClientInfo>,
        client_meta: Option<&livekit::AnalyticsClientMeta>,
        should_send_event: bool,
        guard: Option<Arc<ReferenceGuard>>,
    ) {
        TelemetryServiceImpl::participant_joined(
            self,
            room,
            participant,
            client_info,
            client_meta,
            should_send_event,
            guard,
        );
    }
    fn participant_active(
        &self,
        room: &livekit::Room,
        participant: &livekit::ParticipantInfo,
        client_meta: &livekit::AnalyticsClientMeta,
        is_migration: bool,
        is_warp: bool,
        guard: Option<Arc<ReferenceGuard>>,
    ) {
        TelemetryServiceImpl::participant_active(
            self,
            room,
            participant,
            client_meta,
            is_migration,
            is_warp,
            guard,
        );
    }
    fn participant_resumed(
        &self,
        room: &livekit::Room,
        participant: &livekit::ParticipantInfo,
        node_id: &livekit::NodeId,
        reason: livekit::ReconnectReason,
    ) {
        TelemetryServiceImpl::participant_resumed(self, room, participant, node_id, reason);
    }
    fn participant_left(
        &self,
        room: &livekit::Room,
        participant: &livekit::ParticipantInfo,
        should_send_event: bool,
        guard: Option<Arc<ReferenceGuard>>,
    ) {
        TelemetryServiceImpl::participant_left(self, room, participant, should_send_event, guard);
    }
    fn track_publish_requested(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        track: &livekit::TrackInfo,
        should_send_event: bool,
    ) {
        TelemetryServiceImpl::track_publish_requested(
            self,
            room,
            participant_id,
            identity,
            track,
            should_send_event,
        );
    }
    fn track_published(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        track: &livekit::TrackInfo,
        should_send_event: bool,
    ) {
        TelemetryServiceImpl::track_published(
            self,
            room,
            participant_id,
            identity,
            track,
            should_send_event,
        );
    }
    fn track_unpublished(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        track: &livekit::TrackInfo,
        was_published_locally: bool,
        should_send_event: bool,
    ) {
        TelemetryServiceImpl::track_unpublished(
            self,
            room,
            participant_id,
            identity,
            track,
            was_published_locally,
            should_send_event,
        );
    }
    fn track_subscribe_requested(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
    ) {
        TelemetryServiceImpl::track_subscribe_requested(self, room, participant_id, track);
    }
    fn track_subscribed(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
        publisher: &livekit::ParticipantInfo,
        should_send_event: bool,
    ) {
        TelemetryServiceImpl::track_subscribed(
            self,
            room,
            participant_id,
            track,
            publisher,
            should_send_event,
        );
    }
    fn track_unsubscribed(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
        should_send_event: bool,
    ) {
        TelemetryServiceImpl::track_unsubscribed(
            self,
            room,
            participant_id,
            track,
            should_send_event,
        );
    }
    fn track_subscribe_failed(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        err: &str,
        is_user_error: bool,
    ) {
        TelemetryServiceImpl::track_subscribe_failed(
            self,
            room,
            participant_id,
            track_id,
            err,
            is_user_error,
        );
    }
    fn track_muted(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
    ) {
        TelemetryServiceImpl::track_muted(self, room, participant_id, track);
    }
    fn track_unmuted(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
    ) {
        TelemetryServiceImpl::track_unmuted(self, room, participant_id, track);
    }
    fn track_published_update(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
    ) {
        TelemetryServiceImpl::track_published_update(self, room, participant_id, track);
    }
    fn track_max_subscribed_video_quality(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track: &livekit::TrackInfo,
        mime: MimeType,
        max_quality: livekit::VideoQuality,
    ) {
        TelemetryServiceImpl::track_max_subscribed_video_quality(
            self,
            room,
            participant_id,
            track,
            mime,
            max_quality,
        );
    }
    fn track_publish_rtp_stats(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        mime_type: MimeType,
        layer: i32,
        stats: &livekit::RtpStats,
    ) {
        TelemetryServiceImpl::track_publish_rtp_stats(
            self,
            room,
            participant_id,
            track_id,
            mime_type,
            layer,
            stats,
        );
    }
    fn track_subscribe_rtp_stats(
        &self,
        room: &livekit::Room,
        participant_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        mime_type: MimeType,
        stats: &livekit::RtpStats,
    ) {
        TelemetryServiceImpl::track_subscribe_rtp_stats(
            self,
            room,
            participant_id,
            track_id,
            mime_type,
            stats,
        );
    }
    fn egress_started(&self, info: &livekit::EgressInfo) {
        TelemetryServiceImpl::egress_started(self, info);
    }
    fn egress_updated(&self, info: &livekit::EgressInfo) {
        TelemetryServiceImpl::egress_updated(self, info);
    }
    fn egress_ended(&self, info: &livekit::EgressInfo) {
        TelemetryServiceImpl::egress_ended(self, info);
    }
    fn ingress_created(&self, info: &livekit::IngressInfo) {
        TelemetryServiceImpl::ingress_created(self, info);
    }
    fn ingress_deleted(&self, info: &livekit::IngressInfo) {
        TelemetryServiceImpl::ingress_deleted(self, info);
    }
    fn ingress_started(&self, info: &livekit::IngressInfo) {
        TelemetryServiceImpl::ingress_started(self, info);
    }
    fn ingress_updated(&self, info: &livekit::IngressInfo) {
        TelemetryServiceImpl::ingress_updated(self, info);
    }
    fn ingress_ended(&self, info: &livekit::IngressInfo) {
        TelemetryServiceImpl::ingress_ended(self, info);
    }
    fn local_room_state(&self, info: &livekit::AnalyticsNodeRooms) {
        TelemetryServiceImpl::local_room_state(self, info);
    }
    fn report(&self, report_info: &livekit::ReportInfo) {
        TelemetryServiceImpl::report(self, report_info);
    }
    fn api_call(&self, api_call_info: &livekit::ApiCallInfo) {
        TelemetryServiceImpl::api_call(self, api_call_info);
    }
    fn webhook(&self, webhook_info: &livekit::WebhookInfo) {
        TelemetryServiceImpl::webhook(self, webhook_info);
    }
    fn notify_egress_event(&self, event: &str, info: &livekit::EgressInfo) {
        TelemetryServiceImpl::notify_egress_event(self, event, info);
    }
    fn flush_stats(&self) {
        TelemetryServiceImpl::flush_stats(self);
    }
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
