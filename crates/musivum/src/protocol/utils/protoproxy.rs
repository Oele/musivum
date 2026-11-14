// Ported from LiveKit's protocol, utils/protoproxy.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The message type `T` is anything that can be cloned
// and compared with `==` (which, unlike `proto.Equal`, does not take two NaNs for equal); its
// `Default` is what the zero value of `T` is upstream, where `T` is a pointer and the zero
// value nil: use `ProtoProxy<Option<M>>` to have exactly that. The worker is a task. The
// channels: `Updated()` gives a `Notify` (wait with `.notified().await`), `MarkDirty()` a
// token that is cancelled where upstream closes the channel (wait with
// `.cancelled().await`). `Stop()` cannot wait for a task from a function that is not async,
// so the worker holds the lock `updating` while it updates and `stop` takes it once: when
// `stop` returns no update is running and none will start, which is what waiting for the
// worker gives upstream. The time of a tick is the time it was due. New: the worker holds the
// proxy weakly, and a proxy that is dropped without having been stopped ends its worker and
// lets go of who waits for an update, where Go leaves the goroutine running for ever.

use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::go::time::{self, Duration, Time};

// ProtoProxy is a caching proxy for protobuf messages that may be expensive
// to compute. It is used to avoid unnecessary re-generation of Protobufs
pub struct ProtoProxy<T> {
    update_fn: Box<dyn Fn() -> T + Send + Sync>,
    fuse: CancellationToken,
    update_chan: Notify,
    done: CancellationToken,
    queue_update: Arc<Notify>,
    refresh_interval: Duration,
    // New, see the header.
    updating: Mutex<()>,
    lock: RwLock<ProtoProxyInner<T>>,
}

struct ProtoProxyInner<T> {
    message: T,
    await_chan: Option<CancellationToken>,
    dirty: bool,
    refreshed_at: Time,
}

impl<T: Clone + PartialEq + Default + Send + Sync + 'static> ProtoProxy<T> {
    // NewProtoProxy creates a new ProtoProxy that regenerates underlying values at a cadence of refreshInterval
    // this should be used for updates that should be sent periodically, but does not have the urgency of immediate delivery
    // updateFn should provide computations required to generate the protobuf
    // if refreshInterval is 0, then proxy will only update on MarkDirty(true)
    //
    // New: `update_fn` is kept for as long as the proxy is. If it holds a strong reference to
    // what holds the proxy, the two keep each other alive: give it a `Weak`.
    pub fn new(
        refresh_interval: Duration,
        update_fn: impl Fn() -> T + Send + Sync + 'static,
    ) -> Arc<Self> {
        let p = Arc::new(Self {
            update_chan: Notify::new(),
            update_fn: Box::new(update_fn),
            done: CancellationToken::new(),
            refresh_interval,
            queue_update: Arc::new(Notify::new()),
            fuse: CancellationToken::new(),
            updating: Mutex::new(()),
            lock: RwLock::new(ProtoProxyInner {
                message: T::default(),
                await_chan: None,
                dirty: false,
                refreshed_at: Time::ZERO,
            }),
        });
        p.perform_update(true, time::now());
        if refresh_interval > Duration::ZERO {
            time::spawn(Self::worker(
                Arc::downgrade(&p),
                p.fuse.clone(),
                p.queue_update.clone(),
                p.done.clone(),
                refresh_interval,
            ));
        }
        p
    }

    pub fn mark_dirty(&self, immediate: bool) -> CancellationToken {
        let mut p = self.lock.write();
        p.dirty = true;
        let should_update = immediate || time::since(p.refreshed_at) > self.refresh_interval;

        let await_chan = p
            .await_chan
            .get_or_insert_with(CancellationToken::new)
            .clone();
        drop(p);

        if should_update {
            self.queue_update.notify_one();
        }
        await_chan
    }

    pub fn updated(&self) -> &Notify {
        &self.update_chan
    }

    pub fn get(&self) -> T {
        let p = self.lock.read();
        p.message.clone()
    }

    pub fn stop(&self) {
        self.fuse.cancel();

        // goroutine is not started when refreshInterval == 0
        if self.refresh_interval > Duration::ZERO {
            drop(self.updating.lock());
        }

        let mut p = self.lock.write();
        if let Some(await_chan) = p.await_chan.take() {
            await_chan.cancel();
        }
    }

    fn perform_update(&self, skip_notify: bool, refresh_time: Time) {
        // set dirty back *before* calling updateFn because otherwise it could
        // wipe out another thread setting dirty to true while updateFn is executing
        let mut p = self.lock.write();
        p.dirty = false;

        // Cancelled when this returns: `defer close(awaitChan)` upstream.
        let _await_chan = p.await_chan.take().map(CancellationToken::drop_guard);
        drop(p);

        let msg = (self.update_fn)();

        let mut p = self.lock.write();
        if p.message == msg {
            // no change, skip the notification
            return;
        }
        p.message = msg;
        // only updating refreshedAt if we have notified, so it shouldn't push
        // out the next notification out by another interval
        p.refreshed_at = refresh_time;
        drop(p);

        if !skip_notify {
            self.update_chan.notify_one();
        }
    }

    // What the worker waits on is handed to it, so that it does not keep the proxy alive
    // while it waits.
    async fn worker(
        p: Weak<Self>,
        fuse: CancellationToken,
        queue_update: Arc<Notify>,
        done: CancellationToken,
        refresh_interval: Duration,
    ) {
        let period = refresh_interval.to_std();
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let _done = done.drop_guard();

        loop {
            tokio::select! {
                _ = fuse.cancelled() => {
                    return;
                }
                tick = ticker.tick() => {
                    let now = time::now().add(-Duration::from_std(tick.elapsed()));
                    let Some(p) = p.upgrade() else {
                        return;
                    };
                    let _updating = p.updating.lock();
                    if fuse.is_cancelled() {
                        return;
                    }
                    let inner = p.lock.read();
                    let should_update =
                        inner.dirty && time::since(inner.refreshed_at) >= refresh_interval;
                    drop(inner);
                    if should_update {
                        p.perform_update(false, now);
                    }
                }
                _ = queue_update.notified() => {
                    let Some(p) = p.upgrade() else {
                        return;
                    };
                    let _updating = p.updating.lock();
                    if fuse.is_cancelled() {
                        return;
                    }
                    p.perform_update(false, time::now());
                }
            }
        }
    }
}

// New, see the header.
impl<T> Drop for ProtoProxy<T> {
    fn drop(&mut self) {
        self.fuse.cancel();
        if let Some(await_chan) = self.lock.get_mut().await_chan.take() {
            await_chan.cancel();
        }
    }
}

#[cfg(test)]
#[path = "protoproxy_test.rs"]
mod protoproxy_test;
