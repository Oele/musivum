// Ported from LiveKit, pkg/rtc/supervisor/participant_supervisor.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The supervisor is made as an `Arc`, and the goroutine
// of `checkState` is a task that holds it weakly: it ends at the first tick after `stop`, as
// upstream, and also once nobody holds the supervisor any more. The fields `lock` guards are
// in the lock, the callback with them; it is kept as an `Arc`, since it is copied under the
// lock and called after it, and it gets the track's id by reference. The error kept per
// publication is an `Option` of the package's error as `rtc::Error` passes it on.
// `set_publication_timed_out_for_test` is compiled for tests only. `pub` is a keyword here:
// the variable is `pub_monitor`.
// Who holds whom: the supervisor holds its monitors, and through them each published track,
// strongly; the task holds the supervisor weakly. The callback of `on_publication_error` is
// held strongly for as long as the supervisor lives: its owner hands in one that does not
// hold the owner strongly.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::RwLock;
use tokio::time::MissedTickBehavior;

use super::{PublicationMonitor, PublicationMonitorParams};
use crate::go::atomic;
use crate::go::time::{self, Duration};
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::rtc;
use crate::rtc::types;
use crate::{errorw, infow, warnw};

const MONITOR_INTERVAL: Duration = Duration::from_secs(1);

pub struct ParticipantSupervisorParams {
    pub logger: Logger,
}

// What `on_publication_error` is given, as it is kept.
type OnPublicationError = Arc<dyn Fn(&livekit::TrackId) + Send + Sync>;

struct TrackMonitor {
    op_mon: Arc<dyn types::OperationMonitor>,
    err: Option<rtc::Error>,
}

pub struct ParticipantSupervisor {
    params: ParticipantSupervisorParams,

    // what `lock sync.RWMutex` guards upstream
    inner: RwLock<ParticipantSupervisorInner>,

    is_stopped: atomic::Bool,
}

struct ParticipantSupervisorInner {
    is_publisher_connected: bool,
    publications: HashMap<livekit::TrackId, TrackMonitor>,

    on_publication_error: Option<OnPublicationError>,
}

impl ParticipantSupervisor {
    pub fn new(params: ParticipantSupervisorParams) -> Arc<Self> {
        let p = Arc::new(Self {
            params,
            inner: RwLock::new(ParticipantSupervisorInner {
                is_publisher_connected: false,
                publications: HashMap::new(),
                on_publication_error: None,
            }),
            is_stopped: atomic::Bool::new(false),
        });

        time::spawn(Self::check_state(Arc::downgrade(&p)));

        p
    }

    pub fn stop(&self) {
        self.is_stopped.store(true);
    }

    pub fn on_publication_error(&self, f: impl Fn(&livekit::TrackId) + Send + Sync + 'static) {
        let mut inner = self.inner.write();

        inner.on_publication_error = Some(Arc::new(f));
    }

    fn get_on_publication_error(&self) -> Option<OnPublicationError> {
        let inner = self.inner.read();

        inner.on_publication_error.clone()
    }

    pub fn set_publisher_peer_connection_connected(&self, is_connected: bool) {
        let mut inner = self.inner.write();
        inner.is_publisher_connected = is_connected;

        for pm in inner.publications.values() {
            pm.op_mon.post_event(
                types::OperationMonitorEvent::PublisherPeerConnectionConnected,
                types::OperationMonitorData::Bool(inner.is_publisher_connected),
            );
        }
    }

    pub fn add_publication(&self, track_id: &livekit::TrackId) {
        let mut inner = self.inner.write();
        let is_publisher_connected = inner.is_publisher_connected;
        let pm = inner
            .publications
            .entry(track_id.clone())
            .or_insert_with(|| TrackMonitor {
                op_mon: PublicationMonitor::new(PublicationMonitorParams {
                    track_id: track_id.clone(),
                    is_peer_connection_connected: is_publisher_connected,
                    logger: self.params.logger.clone(),
                }),
                err: None,
            });
        pm.op_mon.post_event(
            types::OperationMonitorEvent::AddPendingPublication,
            types::OperationMonitorData::None,
        );
    }

    // RemovePublication drops the monitor of a pending publication. A monitor that still
    // holds a published track is kept, so a live track never loses its monitor.
    pub fn remove_publication(&self, track_id: &livekit::TrackId) {
        let mut inner = self.inner.write();

        let Some(pm) = inner.publications.get(track_id) else {
            return;
        };
        if let Some(pub_monitor) = as_publication_monitor(&pm.op_mon)
            && pub_monitor.has_published_track()
        {
            warnw!(
                self.params.logger,
                "supervisor: not removing publication with a published track",
                nil,
                "trackID" => track_id,
            );
            return;
        }
        inner.publications.remove(track_id);
    }

    // TimedOutPublications returns the track IDs whose pending publish timed out.
    pub fn timed_out_publications(&self) -> Vec<livekit::TrackId> {
        let inner = self.inner.read();

        let mut track_ids = Vec::new();
        for (track_id, pm) in inner.publications.iter() {
            // pm.err is refreshed once per tick, check again so a track that published since is skipped
            if pm.err.is_some() && pm.op_mon.check().is_err() {
                track_ids.push(track_id.clone());
            }
        }
        track_ids
    }

    // SetPublicationTimedOutForTest puts a publication in the state checkPublications reports
    // once the publish wait passes. Only for tests outside this package.
    #[cfg(test)]
    pub fn set_publication_timed_out_for_test(&self, track_id: &livekit::TrackId) {
        let mut inner = self.inner.write();
        if let Some(pm) = inner.publications.get_mut(track_id) {
            if let Some(pub_monitor) = as_publication_monitor(&pm.op_mon) {
                let mut pub_inner = pub_monitor.inner.write();
                pub_inner.unmuted_at = time::now().add(-2 * super::PUBLISH_WAIT_DURATION);
            }
            pm.err = Some(super::Error::PublishTimeout.into());
        }
    }

    pub fn set_publication_mute(&self, track_id: &livekit::TrackId, is_muted: bool) {
        let inner = self.inner.write();
        if let Some(pm) = inner.publications.get(track_id) {
            pm.op_mon.post_event(
                types::OperationMonitorEvent::SetPublicationMute,
                types::OperationMonitorData::Bool(is_muted),
            );
        }
    }

    pub fn set_published_track(
        &self,
        track_id: &livekit::TrackId,
        pub_track: Arc<dyn types::LocalMediaTrack>,
    ) {
        let inner = self.inner.read();
        if let Some(pm) = inner.publications.get(track_id) {
            pm.op_mon.post_event(
                types::OperationMonitorEvent::SetPublishedTrack,
                types::OperationMonitorData::LocalMediaTrack(pub_track),
            );
        }
    }

    pub fn clear_published_track(
        &self,
        track_id: &livekit::TrackId,
        pub_track: Arc<dyn types::LocalMediaTrack>,
    ) {
        let inner = self.inner.read();
        if let Some(pm) = inner.publications.get(track_id) {
            pm.op_mon.post_event(
                types::OperationMonitorEvent::ClearPublishedTrack,
                types::OperationMonitorData::LocalMediaTrack(pub_track),
            );
        }
    }

    async fn check_state(p: Weak<Self>) {
        let mut ticker = new_ticker(MONITOR_INTERVAL);

        // `for !p.isStopped.Load()`, and for as long as somebody holds the supervisor
        while p.upgrade().is_some_and(|p| !p.is_stopped.load()) {
            ticker.tick().await;

            let Some(p) = p.upgrade() else {
                return;
            };
            p.check_publications();
        }
    }

    fn check_publications(&self) {
        let mut errored_publications = Vec::new();
        let mut removable_publications = Vec::new();
        {
            let mut inner = self.inner.write();
            for (track_id, pm) in inner.publications.iter_mut() {
                match pm.op_mon.check() {
                    Err(err) => {
                        if pm.err.is_none() {
                            errorw!(
                                self.params.logger,
                                "supervisor error on publication",
                                err,
                                "trackID" => track_id,
                            );
                            pm.err = Some(err);
                            errored_publications.push(track_id.clone());
                        }
                    }
                    Ok(()) => {
                        if pm.err.is_some() {
                            infow!(
                                self.params.logger,
                                "supervisor publication recovered",
                                "trackID" => track_id,
                            );
                            pm.err = None;
                        }
                        if pm.op_mon.is_idle() {
                            removable_publications.push(track_id.clone());
                        }
                    }
                }
            }
            for track_id in &removable_publications {
                inner.publications.remove(track_id);
            }
        }

        if let Some(on_publication_error) = self.get_on_publication_error() {
            for track_id in &errored_publications {
                on_publication_error(track_id);
            }
        }
    }
}

// Go's `pm.opMon.(*PublicationMonitor)` with its `ok`.
fn as_publication_monitor(
    op_mon: &Arc<dyn types::OperationMonitor>,
) -> Option<Arc<PublicationMonitor>> {
    let op_mon: Arc<dyn Any + Send + Sync> = op_mon.clone();
    op_mon.downcast::<PublicationMonitor>().ok()
}

// Go's `time.NewTicker(d)`: the first tick comes after `d`, and ticks that the receiver is
// too slow for are dropped.
fn new_ticker(d: Duration) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + d.to_std(), d.to_std());
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker
}

#[cfg(test)]
#[path = "participant_supervisor_test.rs"]
mod participant_supervisor_test;
