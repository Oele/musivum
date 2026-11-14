// Ported from LiveKit, pkg/rtc/signalanddatastats.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - Left out, with LiveKit's observability (`roomobs`), which is not ported: the field
//   `reporter` of `BytesTrackStats` and what `AddBytes` reports to it per message (the kind
//   of the participant and its details, the track type "data", the bytes and one packet in
//   the direction of the message); the fields `kind` and `kindDetails` and the parameters
//   `kind`, `kindDetails` and `participantReporter` of `NewBytesTrackStats`, which exist for
//   those reports only; of `BytesSignalStats` the fields `participantResolver` and
//   `trackResolver`, the deferred reporters that `NewBytesSignalStats` makes from
//   `telemetryService.RoomProjectReporter` (not in the trait here), their `Resolve` in
//   `maybeStart` and their `Reset` in `Reset`. The `context.Context` is dropped, as from the
//   telemetry service.
// - `BytesTrackType`, a string with two values, is an enum whose `Display` gives the value.
// - `NewBytesTrackStats` is `BytesTrackStats::new` and `NewBytesSignalStats` is
//   `BytesSignalStats::new`; both give an `Arc`. `GetTrafficTotals` returns its totals by
//   value.
// - `pID`, `trackID` and `done` of `BytesTrackStats` are behind a lock (`inner`): for the
//   stats of a signal connection they are written after construction (`maybeStart`,
//   `Reset`), upstream as plain fields while no worker runs. `done`, a `core.Fuse`, is a
//   `CancellationToken`.
// - A worker goroutine is a task, its ticker the runtime's interval. The task holds the
//   stats until they are stopped, as the goroutine does: stats that are never stopped are
//   never freed, and neither is their worker.
// - `BytesSignalStats` embeds `BytesTrackStats` and has its `add_bytes`,
//   `get_traffic_totals` and `stop` written out; it embeds
//   `NullParticipantTelemetryListener` and has the trait's methods written out. `mu` guards
//   `stopped`; the guard, the room and the participant, which upstream writes with `mu` held
//   and the worker reads without it, have a lock of their own (`resolved`), which `reset`
//   does not hold while it waits. `stopped`, a channel that the worker closes, is a channel
//   whose sending half the worker drops. `reset` waits for the worker by blocking its
//   thread, as the original blocks its goroutine, and tells the runtime so when the thread
//   is a worker of a multi-threaded one (`block_in_place`); on a current-thread runtime the
//   worker could not run meanwhile: `reset` needs the multi-threaded runtime.
// - `on_track_stats` of `BytesSignalStats` writes the room into a copy of the stat, which it
//   is handed by reference, and does nothing when no room is resolved, where the original
//   dereferences nil: it is called by the worker, which runs while a room is resolved.
// - The tests are new: the original has none.
// Who holds whom: stats keep the closure that gives the telemetry listener (a participant
// that owns its stats hands in one that holds it weakly); the worker task holds the stats
// until `stop`. `BytesSignalStats` holds the telemetry service; the closure of its embedded
// stats holds it weakly.

use std::fmt;
use std::sync::mpsc;
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::go::atomic;
use crate::go::time::{self, Duration, Time};
use crate::protocol::codecs::mime::MimeType;
use crate::protocol::livekit;
use crate::protocol::utils;
use crate::rtc::Error;
use crate::rtc::types::{self, ParticipantTelemetryListener};
use crate::telemetry;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BytesTrackType {
    Data,
    Signal,
}

impl fmt::Display for BytesTrackType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            BytesTrackType::Data => "DT",
            BytesTrackType::Signal => "SG",
        })
    }
}

// -------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TrafficTotals {
    pub at: Time,
    pub send_bytes: u64,
    pub send_messages: u32,
    pub recv_bytes: u64,
    pub recv_messages: u32,
}

// --------------------------------

// stats for signal and data channel
pub struct BytesTrackStats {
    country: String,
    // Not here: kind and kindDetails, which are reported to the reporter only.
    inner: RwLock<BytesTrackStatsInner>,
    send: atomic::Uint64,
    recv: atomic::Uint64,
    send_messages: atomic::Uint32,
    recv_messages: atomic::Uint32,
    total_send_bytes: atomic::Uint64,
    total_recv_bytes: atomic::Uint64,
    total_send_messages: atomic::Uint32,
    total_recv_messages: atomic::Uint32,
    telemetry_listener: Box<dyn Fn() -> Arc<dyn types::ParticipantTelemetryListener> + Send + Sync>,
    // Not here: reporter roomobs.TrackReporter, of LiveKit's observability, which is not
    // ported.
}

// What `BytesSignalStats` writes after construction: see the header.
struct BytesTrackStatsInner {
    p_id: livekit::ParticipantId,
    track_id: livekit::TrackId,
    done: CancellationToken,
}

impl BytesTrackStats {
    pub fn new(
        country: &str,
        track_id: &livekit::TrackId,
        p_id: &livekit::ParticipantId,
        telemetry_listener: impl Fn() -> Arc<dyn types::ParticipantTelemetryListener>
        + Send
        + Sync
        + 'static,
    ) -> Arc<BytesTrackStats> {
        let s = Arc::new(BytesTrackStats {
            country: country.to_string(),
            inner: RwLock::new(BytesTrackStatsInner {
                p_id: p_id.clone(),
                track_id: track_id.clone(),
                done: CancellationToken::new(),
            }),
            send: atomic::Uint64::default(),
            recv: atomic::Uint64::default(),
            send_messages: atomic::Uint32::default(),
            recv_messages: atomic::Uint32::default(),
            total_send_bytes: atomic::Uint64::default(),
            total_recv_bytes: atomic::Uint64::default(),
            total_send_messages: atomic::Uint32::default(),
            total_recv_messages: atomic::Uint32::default(),
            telemetry_listener: Box::new(telemetry_listener),
        });
        time::spawn({
            let s = s.clone();
            async move { s.worker().await }
        });
        s
    }

    pub fn add_bytes(&self, bytes: u64, is_send: bool) {
        if is_send {
            self.send.add(bytes);
            self.send_messages.inc();
            self.total_send_bytes.add(bytes);
            self.total_send_messages.inc();

            // Not here: s.reporter.Tx(...), which reports the participant's kind and its
            // details, the track type, the bytes sent and one packet sent.
        } else {
            self.recv.add(bytes);
            self.recv_messages.inc();
            self.total_recv_bytes.add(bytes);
            self.total_recv_messages.inc();

            // Not here: s.reporter.Tx(...), the same for bytes and a packet received.
        }
    }

    pub fn get_traffic_totals(&self) -> TrafficTotals {
        TrafficTotals {
            at: time::now(),
            send_bytes: self.total_send_bytes.load(),
            send_messages: self.total_send_messages.load(),
            recv_bytes: self.total_recv_bytes.load(),
            recv_messages: self.total_recv_messages.load(),
        }
    }

    pub fn stop(&self) {
        self.inner.read().done.cancel();
    }

    fn report(&self) {
        let recv = self.recv.swap(0);
        if recv > 0 {
            let packets = self.recv_messages.swap(0);
            let (p_id, track_id) = {
                let inner = self.inner.read();
                (inner.p_id.clone(), inner.track_id.clone())
            };
            (self.telemetry_listener)().on_track_stats(
                &telemetry::stats_key_for_data(
                    &self.country,
                    livekit::StreamType::Upstream,
                    p_id,
                    track_id,
                ),
                &livekit::AnalyticsStat {
                    streams: vec![livekit::AnalyticsStream {
                        primary_bytes: recv,
                        primary_packets: packets,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            );
        }

        let send = self.send.swap(0);
        if send > 0 {
            let packets = self.send_messages.swap(0);
            let (p_id, track_id) = {
                let inner = self.inner.read();
                (inner.p_id.clone(), inner.track_id.clone())
            };
            (self.telemetry_listener)().on_track_stats(
                &telemetry::stats_key_for_data(
                    &self.country,
                    livekit::StreamType::Downstream,
                    p_id,
                    track_id,
                ),
                &livekit::AnalyticsStat {
                    streams: vec![livekit::AnalyticsStream {
                        primary_bytes: send,
                        primary_packets: packets,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            );
        }
    }

    async fn worker(&self) {
        let done = self.inner.read().done.clone();

        // Go's `time.NewTicker(5 * time.Second)`: the first tick comes after the interval,
        // and ticks that the receiver is too slow for are dropped.
        let interval: Duration = 5 * time::SECOND;
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + interval.to_std(),
            interval.to_std(),
        );
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = done.cancelled() => {
                    break;
                }
                _ = ticker.tick() => {
                    self.report();
                }
            }
        }

        // what the original defers: ticker.Stop() and
        self.report();
    }
}

// -----------------------------------------------------------------------

pub struct BytesSignalStats {
    bytes_track_stats: BytesTrackStats,

    telemetry: Arc<dyn telemetry::TelemetryService>,

    // Not here: participantResolver roomobs.ParticipantReporterResolver and trackResolver
    // roomobs.KeyResolver, of LiveKit's observability, which is not ported.

    // `mu sync.Mutex`, with what only its holders touch: `stopped`
    mu: Mutex<Option<mpsc::Receiver<()>>>,
    // what is written with `mu` held and read by the worker without it
    resolved: RwLock<BytesSignalStatsResolved>,

    null_participant_telemetry_listener: types::NullParticipantTelemetryListener,

    this: Weak<BytesSignalStats>,
}

struct BytesSignalStatsResolved {
    guard: Arc<telemetry::ReferenceGuard>,
    ri: Option<livekit::Room>,
    pi: Option<livekit::ParticipantInfo>,
}

impl BytesSignalStats {
    pub fn new(telemetry_service: Arc<dyn telemetry::TelemetryService>) -> Arc<BytesSignalStats> {
        // Not here: the deferred reporters of the participant and of the track, made from
        // telemetryService.RoomProjectReporter(ctx).
        Arc::new_cyclic(|this: &Weak<BytesSignalStats>| {
            let b = this.clone();
            BytesSignalStats {
                bytes_track_stats: BytesTrackStats {
                    country: String::new(),
                    inner: RwLock::new(BytesTrackStatsInner {
                        p_id: livekit::ParticipantId::default(),
                        track_id: livekit::TrackId::default(),
                        done: CancellationToken::new(),
                    }),
                    send: atomic::Uint64::default(),
                    recv: atomic::Uint64::default(),
                    send_messages: atomic::Uint32::default(),
                    recv_messages: atomic::Uint32::default(),
                    total_send_bytes: atomic::Uint64::default(),
                    total_recv_bytes: atomic::Uint64::default(),
                    total_send_messages: atomic::Uint32::default(),
                    total_recv_messages: atomic::Uint32::default(),
                    telemetry_listener: Box::new(move || BytesSignalStats::telemetry_listener(&b)),
                },
                telemetry: telemetry_service,
                mu: Mutex::new(None),
                resolved: RwLock::new(BytesSignalStatsResolved {
                    guard: Arc::new(telemetry::ReferenceGuard::default()),
                    ri: None,
                    pi: None,
                }),
                null_participant_telemetry_listener: types::NullParticipantTelemetryListener {},
                this: this.clone(),
            }
        })
    }

    // The methods of the embedded `BytesTrackStats`.
    pub fn add_bytes(&self, bytes: u64, is_send: bool) {
        self.bytes_track_stats.add_bytes(bytes, is_send);
    }

    pub fn get_traffic_totals(&self) -> TrafficTotals {
        self.bytes_track_stats.get_traffic_totals()
    }

    pub fn stop(&self) {
        self.bytes_track_stats.stop();
    }

    pub fn resolve_room(&self, ri: Option<&livekit::Room>) {
        let mut stopped = self.mu.lock();
        let mut resolved = self.resolved.write();
        if resolved.ri.is_none()
            && let Some(ri) = ri
            && !ri.sid.is_empty()
        {
            resolved.ri = Some(livekit::Room {
                sid: ri.sid.clone(),
                name: ri.name.clone(),
                ..Default::default()
            });
            drop(resolved);
            self.maybe_start(&mut stopped);
        }
    }

    pub fn resolve_participant(&self, pi: Option<&livekit::ParticipantInfo>) {
        let mut stopped = self.mu.lock();
        let mut resolved = self.resolved.write();
        if resolved.pi.is_none()
            && let Some(pi) = pi
        {
            resolved.pi = Some(livekit::ParticipantInfo {
                sid: pi.sid.clone(),
                identity: pi.identity.clone(),
                kind: pi.kind,
                kind_details: pi.kind_details.clone(),
                ..Default::default()
            });
            // Not here: s.kind = pi.Kind and s.kindDetails = pi.KindDetails, for the reporter.
            drop(resolved);
            self.maybe_start(&mut stopped);
        }
    }

    pub fn reset(&self) {
        let mut stopped = self.mu.lock();
        if let Some(s) = stopped.take() {
            self.bytes_track_stats.stop();
            wait_stopped(s);
            self.bytes_track_stats.inner.write().done = CancellationToken::new();
            self.resolved.write().guard = Arc::new(telemetry::ReferenceGuard::default());
        }
        {
            let mut resolved = self.resolved.write();
            resolved.ri = None;
            resolved.pi = None;
        }

        // Not here: s.participantResolver.Reset() and s.trackResolver.Reset().
    }

    // Called with `mu` held: `stopped` is what it guards.
    fn maybe_start(&self, stopped: &mut Option<mpsc::Receiver<()>>) {
        let (ri, pi, guard) = {
            let resolved = self.resolved.read();
            let (Some(ri), Some(pi)) = (&resolved.ri, &resolved.pi) else {
                return;
            };
            (ri.clone(), pi.clone(), resolved.guard.clone())
        };
        let Some(s) = self.this.upgrade() else {
            return;
        };

        {
            let mut inner = self.bytes_track_stats.inner.write();
            inner.p_id = livekit::ParticipantId::from(&pi.sid);
            inner.track_id = bytes_track_id_for_participant_id(BytesTrackType::Signal, &inner.p_id);
        }

        // Not here: s.participantResolver.Resolve(room name, room ID, participant identity,
        // participant ID) and s.trackResolver.Resolve(track ID).

        self.telemetry
            .participant_joined(&ri, &pi, None, None, false, Some(guard));
        let (stopped_tx, stopped_rx) = mpsc::channel();
        *stopped = Some(stopped_rx);
        time::spawn(s.worker(stopped_tx));
    }

    async fn worker(self: Arc<Self>, stopped: mpsc::Sender<()>) {
        self.bytes_track_stats.worker().await;
        let (ri, pi, guard) = {
            let resolved = self.resolved.read();
            (
                resolved.ri.clone(),
                resolved.pi.clone(),
                resolved.guard.clone(),
            )
        };
        if let (Some(ri), Some(pi)) = (ri, pi) {
            self.telemetry
                .participant_left(&ri, &pi, false, Some(guard));
        }
        // close(s.stopped)
        drop(stopped);
    }

    fn telemetry_listener(
        this: &Weak<BytesSignalStats>,
    ) -> Arc<dyn types::ParticipantTelemetryListener> {
        match this.upgrade() {
            Some(s) => s,
            // not reached: it is the worker that asks, and the worker holds the stats
            None => Arc::new(types::NullParticipantTelemetryListener {}),
        }
    }
}

// `<-s.stopped`: see the header.
fn wait_stopped(stopped: mpsc::Receiver<()>) {
    let wait = || {
        // an error is the worker having dropped its half: the channel is closed
        let _ = stopped.recv();
    };
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(wait);
        }
        _ => wait(),
    }
}

impl ParticipantTelemetryListener for BytesSignalStats {
    // The methods of the embedded `NullParticipantTelemetryListener`.
    fn on_track_publish_requested(
        &self,
        p_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        ti: &livekit::TrackInfo,
        should_send_event: bool,
    ) {
        self.null_participant_telemetry_listener
            .on_track_publish_requested(p_id, identity, ti, should_send_event);
    }
    fn on_track_published(
        &self,
        p_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        ti: &livekit::TrackInfo,
        should_send_event: bool,
    ) {
        self.null_participant_telemetry_listener.on_track_published(
            p_id,
            identity,
            ti,
            should_send_event,
        );
    }
    fn on_track_unpublished(
        &self,
        p_id: &livekit::ParticipantId,
        identity: &livekit::ParticipantIdentity,
        ti: &livekit::TrackInfo,
        was_published_locally: bool,
        should_send_event: bool,
    ) {
        self.null_participant_telemetry_listener
            .on_track_unpublished(p_id, identity, ti, was_published_locally, should_send_event);
    }
    fn on_track_subscribe_requested(&self, p_id: &livekit::ParticipantId, ti: &livekit::TrackInfo) {
        self.null_participant_telemetry_listener
            .on_track_subscribe_requested(p_id, ti);
    }
    fn on_track_subscribed(
        &self,
        p_id: &livekit::ParticipantId,
        ti: &livekit::TrackInfo,
        publisher_info: &livekit::ParticipantInfo,
        should_send_event: bool,
    ) {
        self.null_participant_telemetry_listener
            .on_track_subscribed(p_id, ti, publisher_info, should_send_event);
    }
    fn on_track_unsubscribed(
        &self,
        p_id: &livekit::ParticipantId,
        ti: &livekit::TrackInfo,
        should_send_event: bool,
    ) {
        self.null_participant_telemetry_listener
            .on_track_unsubscribed(p_id, ti, should_send_event);
    }
    fn on_track_subscribe_failed(
        &self,
        p_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        err: &Error,
        is_user_error: bool,
    ) {
        self.null_participant_telemetry_listener
            .on_track_subscribe_failed(p_id, track_id, err, is_user_error);
    }
    fn on_track_subscribe_stream_started(
        &self,
        p_id: &livekit::ParticipantId,
        ti: &livekit::TrackInfo,
        elapsed: Duration,
    ) {
        self.null_participant_telemetry_listener
            .on_track_subscribe_stream_started(p_id, ti, elapsed);
    }
    fn on_track_muted(&self, p_id: &livekit::ParticipantId, ti: &livekit::TrackInfo) {
        self.null_participant_telemetry_listener
            .on_track_muted(p_id, ti);
    }
    fn on_track_unmuted(&self, p_id: &livekit::ParticipantId, ti: &livekit::TrackInfo) {
        self.null_participant_telemetry_listener
            .on_track_unmuted(p_id, ti);
    }
    fn on_track_published_update(&self, p_id: &livekit::ParticipantId, ti: &livekit::TrackInfo) {
        self.null_participant_telemetry_listener
            .on_track_published_update(p_id, ti);
    }
    fn on_track_max_subscribed_video_quality(
        &self,
        p_id: &livekit::ParticipantId,
        ti: &livekit::TrackInfo,
        mime: MimeType,
        max_quality: livekit::VideoQuality,
    ) {
        self.null_participant_telemetry_listener
            .on_track_max_subscribed_video_quality(p_id, ti, mime, max_quality);
    }
    fn on_track_publish_rtp_stats(
        &self,
        p_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        mime_type: MimeType,
        layer: i32,
        stats: &livekit::RtpStats,
    ) {
        self.null_participant_telemetry_listener
            .on_track_publish_rtp_stats(p_id, track_id, mime_type, layer, stats);
    }
    fn on_track_subscribe_rtp_stats(
        &self,
        p_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
        mime_type: MimeType,
        stats: &livekit::RtpStats,
    ) {
        self.null_participant_telemetry_listener
            .on_track_subscribe_rtp_stats(p_id, track_id, mime_type, stats);
    }

    fn on_track_stats(&self, key: &telemetry::StatsKey, stat: &livekit::AnalyticsStat) {
        let Some(ri) = self.resolved.read().ri.clone() else {
            return;
        };
        let mut stat = stat.clone();
        stat.room_id = ri.sid.clone();
        stat.room_name = ri.name.clone();
        self.telemetry.track_stats(
            &livekit::RoomId::from(&ri.sid),
            &livekit::RoomName::from(&ri.name),
            key,
            &stat,
        );
    }
}

// -----------------------------------------------------------------------

pub fn bytes_track_id_for_participant_id(
    typ: BytesTrackType,
    participant_id: &livekit::ParticipantId,
) -> livekit::TrackId {
    livekit::TrackId::from(format!("{}{}{}", utils::TRACK_PREFIX, typ, participant_id))
}

#[cfg(test)]
#[path = "signalanddatastats_test.rs"]
mod signalanddatastats_test;
