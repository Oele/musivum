// Ported from LiveKit's protocol, webhook/resource_queue.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The worker is a task, and `Process` of the poster,
// which sends, a future it awaits; the condition variable is a `tokio::sync::Notify` that the
// worker registers with before it looks at the queue. The `context.Context` of an item is
// dropped. An item has the event itself and the parameters in an `Arc`, which the notifier
// shares. `Enqueue` and `EnqueueAt` return the length of the queue in the `Ok` (the original
// returns it with the error too, where nothing reads it).

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

use super::Error;
use super::resource_url_notifier::{Poster, ResourceUrlNotifierParams};
use crate::go::time::{self, Time};
use crate::protocol::livekit;

struct Item {
    queued_at: Time,
    event: livekit::WebhookEvent,
    params: Arc<ResourceUrlNotifierParams>,
}

pub(super) struct ResourceQueueParams {
    pub(super) max_depth: i64,

    pub(super) poster: Arc<dyn Poster>,
}

pub(super) struct ResourceQueue {
    params: ResourceQueueParams,

    mu: Mutex<ResourceQueueInner>,
    cond: Notify,
}

struct ResourceQueueInner {
    items: VecDeque<Item>,

    closed: bool,
    drain: bool,
}

pub(super) fn new_resource_queue(params: ResourceQueueParams) -> Arc<ResourceQueue> {
    let base_cap = params.max_depth.clamp(0, 16) as usize;
    let r = Arc::new(ResourceQueue {
        params,
        mu: Mutex::new(ResourceQueueInner {
            items: VecDeque::with_capacity(base_cap),
            closed: false,
            drain: false,
        }),
        cond: Notify::new(),
    });

    let worker = r.clone();
    time::spawn(async move { worker.worker().await });
    r
}

impl ResourceQueue {
    pub(super) fn stop(&self, force: bool) {
        let mut r = self.mu.lock();

        if !r.closed {
            r.closed = true;
            r.drain = !force;

            self.cond.notify_waiters();
        }
    }

    pub(super) fn enqueue(
        &self,
        wh_event: &livekit::WebhookEvent,
        params: &Arc<ResourceUrlNotifierParams>,
    ) -> Result<usize, Error> {
        self.enqueue_at(time::now(), wh_event, params)
    }

    pub(super) fn enqueue_at(
        &self,
        at: Time,
        wh_event: &livekit::WebhookEvent,
        params: &Arc<ResourceUrlNotifierParams>,
    ) -> Result<usize, Error> {
        let mut r = self.mu.lock();

        if r.closed {
            return Err(Error::QueueClosed);
        }

        if r.items.len() as i64 >= self.params.max_depth {
            return Err(Error::QueueFull);
        }

        r.items.push_back(Item {
            queued_at: at,
            event: wh_event.clone(),
            params: params.clone(),
        });
        self.cond.notify_waiters();
        Ok(r.items.len())
    }

    async fn worker(&self) {
        loop {
            let (item, q_len) = loop {
                let notified = self.cond.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let mut r = self.mu.lock();
                    if r.closed && (!r.drain || r.items.is_empty()) {
                        return;
                    }

                    if let Some(item) = r.items.pop_front() {
                        let q_len = r.items.len();
                        break (item, q_len);
                    }
                }
                notified.await;
            };

            self.params
                .poster
                .process(item.queued_at, item.event, item.params, q_len)
                .await;
        }
    }
}
