// Ported from LiveKit, pkg/rtc/supervisor/publication_monitor.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. `errPublishTimeout` is the one variant of `Error`,
// the errors of the package, which `rtc::Error` passes on (`check` returns that one, as the
// trait says). The monitor is made as an `Arc`: the supervisor holds it as a
// `types::OperationMonitor`. The fields `lock` guards are in the lock, and the two functions
// called with it held take the locked state. The deque holds the publishes by value. What
// comes with an event is the enum `types::OperationMonitorData`; an event posted with the
// wrong kind of data panics, as the original's type assertions do. `pub` is a keyword here:
// the variable is `publish`.
// Who holds whom: the monitor holds the published track strongly, from the event that sets
// it to the event that clears it, as the original does (it compares it and asks whether
// there is one).

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::errorw;
use crate::go::time::{self, Duration, Time};
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::rtc;
use crate::rtc::types;

pub(super) const PUBLISH_WAIT_DURATION: Duration = Duration::from_secs(30);

/// The errors of the package: its `errX` values, with the original's messages.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("publish time out")]
    PublishTimeout,
}

#[derive(Clone, Copy)]
struct Publish {
    is_start: bool,
}

pub struct PublicationMonitorParams {
    pub track_id: livekit::TrackId,
    pub is_peer_connection_connected: bool,
    pub logger: Logger,
}

pub struct PublicationMonitor {
    params: PublicationMonitorParams,

    // what `lock sync.RWMutex` guards upstream
    pub(super) inner: RwLock<PublicationMonitorInner>,
}

pub(super) struct PublicationMonitorInner {
    desired_publishes: VecDeque<Publish>,

    is_connected: bool,

    published_track: Option<Arc<dyn types::LocalMediaTrack>>,
    is_muted: bool,
    pub(super) unmuted_at: Time,
}

impl PublicationMonitor {
    pub fn new(params: PublicationMonitorParams) -> Arc<Self> {
        let is_connected = params.is_peer_connection_connected;
        Arc::new(Self {
            params,
            inner: RwLock::new(PublicationMonitorInner {
                // `p.desiredPublishes.SetBaseCap(4)`
                desired_publishes: VecDeque::with_capacity(4),
                is_connected,
                published_track: None,
                is_muted: false,
                unmuted_at: Time::ZERO,
            }),
        })
    }

    pub fn post_event(&self, ome: types::OperationMonitorEvent, omd: types::OperationMonitorData) {
        match ome {
            types::OperationMonitorEvent::PublisherPeerConnectionConnected => {
                self.set_connected(data_bool(omd));
            }
            types::OperationMonitorEvent::AddPendingPublication => self.add_pending(),
            types::OperationMonitorEvent::SetPublicationMute => self.set_mute(data_bool(omd)),
            types::OperationMonitorEvent::SetPublishedTrack => {
                self.set_published_track(data_local_media_track(omd));
            }
            types::OperationMonitorEvent::ClearPublishedTrack => {
                self.clear_published_track(data_local_media_track(omd));
            }
        }
    }

    fn add_pending(&self) {
        let mut inner = self.inner.write();
        inner
            .desired_publishes
            .push_back(Publish { is_start: true });

        // synthesize an end
        inner
            .desired_publishes
            .push_back(Publish { is_start: false });
        Self::update(&mut inner);
    }

    fn maybe_start_monitor(inner: &mut PublicationMonitorInner) {
        if inner.is_connected && !inner.is_muted {
            inner.unmuted_at = time::now();
        }
    }

    fn set_connected(&self, is_connected: bool) {
        let mut inner = self.inner.write();
        inner.is_connected = is_connected;
        Self::maybe_start_monitor(&mut inner);
    }

    fn set_mute(&self, is_muted: bool) {
        let mut inner = self.inner.write();
        inner.is_muted = is_muted;
        Self::maybe_start_monitor(&mut inner);
    }

    fn set_published_track(&self, pub_track: Arc<dyn types::LocalMediaTrack>) {
        let mut inner = self.inner.write();
        inner.published_track = Some(pub_track);
        Self::update(&mut inner);
    }

    fn clear_published_track(&self, pub_track: Arc<dyn types::LocalMediaTrack>) {
        let mut inner = self.inner.write();
        if inner
            .published_track
            .as_ref()
            .is_some_and(|published_track| Arc::ptr_eq(published_track, &pub_track))
        {
            inner.published_track = None;
        } else {
            errorw!(
                self.params.logger,
                "supervisor: mismatched published track on clear",
                nil,
                "trackID" => self.params.track_id,
            );
        }

        Self::update(&mut inner);
    }

    pub fn check(&self) -> Result<(), rtc::Error> {
        let inner = self.inner.read();
        let publish = inner.desired_publishes.front().copied();

        let is_muted = inner.is_muted;
        let unmuted_at = inner.unmuted_at;
        drop(inner);

        let Some(publish) = publish else {
            return Ok(());
        };

        if publish.is_start
            && !is_muted
            && !unmuted_at.is_zero()
            && time::since(unmuted_at) > PUBLISH_WAIT_DURATION
        {
            // timed out waiting for publish
            return Err(Error::PublishTimeout.into());
        }

        // give more time for publish to happen
        // NOTE: synthesized end events do not have a start time, so do not check them for time out
        Ok(())
    }

    pub(super) fn has_published_track(&self) -> bool {
        let inner = self.inner.read();

        inner.published_track.is_some()
    }

    pub fn is_idle(&self) -> bool {
        let inner = self.inner.read();

        inner.desired_publishes.is_empty() && inner.published_track.is_none()
    }

    // update is the only place the front of desiredPublishes moves.
    // Any other pop must also re-arm the publish timer.
    fn update(inner: &mut PublicationMonitorInner) {
        let mut popped = false;
        loop {
            let Some(publish) = inner.desired_publishes.pop_front() else {
                return;
            };

            if (publish.is_start && inner.published_track.is_none())
                || (!publish.is_start && inner.published_track.is_some())
            {
                // put it back as the condition is not satisfied
                inner.desired_publishes.push_front(publish);

                // a queued publish reached the front, start its publish window from now
                if popped && publish.is_start {
                    inner.unmuted_at = Time::ZERO;
                    Self::maybe_start_monitor(inner);
                }
                return;
            }
            popped = true;
        }
    }
}

impl types::OperationMonitor for PublicationMonitor {
    fn post_event(&self, ome: types::OperationMonitorEvent, omd: types::OperationMonitorData) {
        PublicationMonitor::post_event(self, ome, omd);
    }

    fn check(&self) -> Result<(), rtc::Error> {
        PublicationMonitor::check(self)
    }

    fn is_idle(&self) -> bool {
        PublicationMonitor::is_idle(self)
    }
}

// Go's `omd.(bool)`, which panics when the data is something else.
fn data_bool(omd: types::OperationMonitorData) -> bool {
    match omd {
        types::OperationMonitorData::Bool(b) => b,
        _ => panic!("interface conversion: the data of the event is not a bool"),
    }
}

// Go's `omd.(types.LocalMediaTrack)`, which panics when the data is something else.
fn data_local_media_track(omd: types::OperationMonitorData) -> Arc<dyn types::LocalMediaTrack> {
    match omd {
        types::OperationMonitorData::LocalMediaTrack(track) => track,
        _ => panic!("interface conversion: the data of the event is not a local media track"),
    }
}

#[cfg(test)]
#[path = "publication_monitor_test.rs"]
mod publication_monitor_test;
