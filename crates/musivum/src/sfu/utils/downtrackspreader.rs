// Ported from LiveKit, pkg/sfu/utils/downtrackspreader.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The two unexported interfaces (`sender`, `rtpWriter`)
// are public traits, since a bound has to be named here, and are implemented for an `Arc` of
// what implements them, which is what a spreader holds where Go holds pointers. The shadow
// slice is an `Arc<[T]>`, so that it is handed out without copying as Go's slice is. The
// params are inside the lock, as `SetThreshold` changes them under it; the threshold is
// unsigned. The functions called with the lock held take the locked state. The writer of
// `broadcast` and `write_rtp` get the down track and the packet by reference. The workers of
// `broadcast_rtp` are jobs on the threads of `go::sync::go`, which sleep as soon as there is
// nothing to do, where the original starts goroutines on Go's threads (they were jobs on
// rayon's pool, whose threads look for work for a while after each job, and that cost more
// than the writes did); the shared state is in an `Arc` with a copy of the packet, as a job
// there outlives the borrow (Go shares it by pointer). They are started as Go's scheduler
// gets its threads to the goroutines, not all at once: the caller is the first worker (in Go
// the thread of the caller runs the goroutines while the caller waits), and a worker that
// takes work while there is more left starts one more, as long as no other is on its way and
// fewer than `numWorkers` were started (Go wakes another thread when a woken one has found
// work, and only while none is looking for it). The `WaitGroup` counts a worker when it
// begins, so that the caller does not wait for one that begins after all the work was
// taken; a worker that panicked makes the caller panic.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::go::atomic;
use crate::go::sync::{WaitGroup, go};
use crate::protocol::livekit::ParticipantId;
use crate::protocol::logger::Logger;
use crate::protocol::utils;

// 100µs is enough to amortize the overhead and provide sufficient load balancing.
// WriteRTP takes about 50µs on average, so we write to 2 down tracks per loop.
const BROADCAST_STEP: u64 = 2;

#[derive(Clone, Debug, Default)]
pub struct DownTrackSpreaderParams {
    pub threshold: usize,
    pub logger: Logger,
}

pub struct DownTrackSpreader<T: Sender> {
    // what `downTrackMu sync.RWMutex` guards upstream
    inner: RwLock<DownTrackSpreaderInner<T>>,
}

struct DownTrackSpreaderInner<T: Sender> {
    params: DownTrackSpreaderParams,

    down_tracks: HashMap<ParticipantId, T>,
    down_tracks_shadow: Arc<[T]>,
    closed: bool,
}

impl<T: Sender + Clone> DownTrackSpreader<T> {
    pub fn new(params: DownTrackSpreaderParams) -> Self {
        Self {
            inner: RwLock::new(DownTrackSpreaderInner {
                params,
                down_tracks: HashMap::new(),
                down_tracks_shadow: Arc::default(),
                closed: false,
            }),
        }
    }

    pub fn get_down_tracks(&self) -> Arc<[T]> {
        let inner = self.inner.read();
        inner.down_tracks_shadow.clone()
    }

    pub fn reset_and_get_down_tracks(&self) -> Arc<[T]> {
        let mut inner = self.inner.write();

        Self::reset_and_get_down_tracks_locked(&mut inner)
    }

    // CloseAndGetDownTracks terminally closes the spreader and drains all senders.
    // Once it returns, TryStore will reject every subsequent sender.
    pub fn close_and_get_down_tracks(&self) -> Arc<[T]> {
        let mut inner = self.inner.write();

        inner.closed = true;
        Self::reset_and_get_down_tracks_locked(&mut inner)
    }

    pub fn store(&self, sender: T) {
        let _ = self.try_store(sender);
    }

    // TryStore stores sender unless the spreader has been terminally closed.
    pub fn try_store(&self, sender: T) -> bool {
        let mut inner = self.inner.write();

        if inner.closed {
            return false;
        }

        inner.down_tracks.insert(sender.subscriber_id(), sender);
        Self::shadow_down_tracks(&mut inner);
        true
    }

    pub fn free(&self, subscriber_id: &ParticipantId) {
        let mut inner = self.inner.write();

        inner.down_tracks.remove(subscriber_id);
        Self::shadow_down_tracks(&mut inner);
    }

    pub fn has_down_track(&self, subscriber_id: &ParticipantId) -> bool {
        let inner = self.inner.read();

        inner.down_tracks.contains_key(subscriber_id)
    }

    // snapshot returns the current down tracks and the parallelization threshold
    fn snapshot(&self) -> (Arc<[T]>, usize) {
        let inner = self.inner.read();
        let down_tracks = inner.down_tracks_shadow.clone();
        let mut threshold = inner.params.threshold;
        drop(inner);

        if threshold == 0 {
            threshold = 1000000;
        }
        (down_tracks, threshold)
    }

    pub fn broadcast(&self, writer: impl Fn(&T) + Sync) {
        let (down_tracks, threshold) = self.snapshot();
        if down_tracks.is_empty() {
            return;
        }

        utils::parallel_exec(&down_tracks, threshold as u64, BROADCAST_STEP, writer);
    }

    pub fn down_track_count(&self) -> usize {
        let inner = self.inner.read();
        inner.down_tracks_shadow.len()
    }

    fn reset_and_get_down_tracks_locked(inner: &mut DownTrackSpreaderInner<T>) -> Arc<[T]> {
        let down_tracks = std::mem::take(&mut inner.down_tracks_shadow);
        inner.down_tracks = HashMap::new();
        down_tracks
    }

    fn shadow_down_tracks(inner: &mut DownTrackSpreaderInner<T>) {
        let mut down_tracks_shadow = Vec::with_capacity(inner.down_tracks.len());
        for dt in inner.down_tracks.values() {
            down_tracks_shadow.push(dt.clone());
        }
        inner.down_tracks_shadow = Arc::from(down_tracks_shadow);
    }

    pub fn set_threshold(&self, threshold: usize) {
        let mut inner = self.inner.write();
        inner.params.threshold = threshold;
    }
}

// ------------------------------------------------

pub trait Sender: Send + Sync {
    fn subscriber_id(&self) -> ParticipantId;
}

impl<S: Sender + ?Sized> Sender for Arc<S> {
    fn subscriber_id(&self) -> ParticipantId {
        (**self).subscriber_id()
    }
}

pub trait RtpWriter<P: ?Sized>: Send + Sync {
    fn write_rtp(&self, pkt: &P, layer: i32) -> i32;
}

impl<P: ?Sized, S: RtpWriter<P> + ?Sized> RtpWriter<P> for Arc<S> {
    fn write_rtp(&self, pkt: &P, layer: i32) -> i32 {
        (**self).write_rtp(pkt, layer)
    }
}

// rtpBroadcast is the shared state of one parallel BroadcastRTP, it carries the
// packet and layer so that no closure has to be allocated per packet
struct RtpBroadcast<T, P> {
    down_tracks: Arc<[T]>,
    pkt: P,
    layer: i32,
    next: atomic::Uint64,
    written: atomic::Int32,
    wg: WaitGroup,
    // New: how many workers may still be started, and whether one was started that has not
    // begun yet (see the header)
    to_start: atomic::Uint64,
    starting: atomic::Bool,
}

impl<T, P> RtpBroadcast<T, P>
where
    T: RtpWriter<P> + 'static,
    P: Send + Sync + 'static,
{
    fn run(self: &Arc<Self>) {
        let mut written: i32 = 0;
        let end = self.down_tracks.len() as u64;
        loop {
            let n = self.next.add(BROADCAST_STEP);
            if n >= end + BROADCAST_STEP {
                break;
            }
            if n < end {
                self.start_worker();
            }
            let mut i = n - BROADCAST_STEP;
            while i < n && i < end {
                written = written
                    .wrapping_add(self.down_tracks[i as usize].write_rtp(&self.pkt, self.layer));
                i += 1;
            }
        }
        self.written.add(written);
    }

    // New: starts one more worker, unless one is on its way or all were started
    fn start_worker(self: &Arc<Self>) {
        if self.starting.load() || !self.starting.cas(false, true) {
            return;
        }
        if self.to_start.load() == 0 {
            // `starting` stays set: there is nothing more to start
            return;
        }
        self.to_start.dec();
        let b = self.clone();
        go(move || {
            b.wg.add(1);
            let _done = b.wg.defer_done();
            b.starting.store(false);
            b.run();
        });
    }
}

// BroadcastRTP writes pkt to every down track and returns how many accepted it.
// Below the threshold it runs on the caller without allocating; above it, the
// only allocations are the shared state (with a copy of the packet) and the workers.
pub fn broadcast_rtp<T, P>(d: &DownTrackSpreader<T>, pkt: &P, layer: i32) -> i32
where
    T: Sender + RtpWriter<P> + Clone + 'static,
    P: Clone + Send + Sync + 'static,
{
    let (down_tracks, threshold) = d.snapshot();
    if down_tracks.len() < threshold {
        let mut written: i32 = 0;
        for dt in down_tracks.iter() {
            written = written.wrapping_add(dt.write_rtp(pkt, layer));
        }
        return written;
    }

    let num_workers = utils::num_cpu().min(down_tracks.len());
    let b = Arc::new(RtpBroadcast {
        down_tracks,
        pkt: pkt.clone(),
        layer,
        next: atomic::Uint64::new(0),
        written: atomic::Int32::new(0),
        wg: WaitGroup::new(),
        to_start: atomic::Uint64::new(num_workers.saturating_sub(1) as u64),
        starting: atomic::Bool::new(false),
    });
    b.run();
    if b.wg.wait() {
        panic!("a worker of broadcast_rtp panicked");
    }

    b.written.load()
}

#[cfg(test)]
#[path = "downtrackspreader_test.rs"]
mod downtrackspreader_test;
