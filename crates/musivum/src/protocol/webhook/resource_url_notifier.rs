// Ported from LiveKit's protocol, webhook/resource_url_notifier.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The `context.Context` of `QueueNotify`, of `Process`
// and of the processed hook is dropped. The errors `errClosed` and `errNoKey` are variants of
// the package's `Error` (`consts.rs`). `NewResourceURLNotifier` returns an `Arc`, and the
// notifier is the poster of its queues through it: the queues and the sweeper keep it, so it
// lives until it is stopped, as upstream it is not collected before. `resourceQueueInfo` and
// its `TimeoutQueueItem`, which point at each other upstream, are one object here: the item
// of the timeout queue whose value is the information (`Rqi`), which the map holds. The
// embedded `HTTPClientParams` and `FilterParams` are fields named after them; a nil `Logger`
// has no counterpart (the default is `logger::get_logger()`); `FieldsHook` and `EventKey` are
// optional closures in `Arc`s, so that the parameters can be copied as the original copies
// them. The filter and the event key are read under the lock that guards their changes,
// where the original reads them without. `Process` and `send` are futures, the HTTP client
// is `retryablehttp.rs`. The sweeper is a task with a ticker that skips the ticks it misses;
// it reads the timeout it was started with. The closed fuse is a `CancellationToken`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use base64::Engine as _;
use bytes::Bytes;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::filter::{Filter, new_filter};
use super::notifier::{
    EventKeyFn, FieldsHook, FilterParams, HttpClientParams, NotifyOption, NotifyParams,
    ProcessedHook, QueuedNotifier, event_key, log_fields, log_kvs, webhook_info,
};
use super::resource_queue::{ResourceQueue, ResourceQueueParams, new_resource_queue};
use super::retryablehttp::{self, Response};
use super::stats::{
    inc_dispatch_drop, inc_dispatch_failure, inc_dispatch_success, record_queue_length,
};
use super::{
    AUTH_HEADER, CONTENT_TYPE, CONTENT_TYPE_HEADER, DEFAULT_USER_AGENT, Error, USER_AGENT_HEADER,
};
use crate::go::time::{self, Duration, MINUTE, SECOND, Time};
use crate::protocol::auth::AccessToken;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::protocol::utils::{TimeoutQueue, TimeoutQueueItem};

const DEFAULT_TIMEOUT: Duration = Duration(5 * MINUTE.0);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResourceUrlNotifierConfig {
    /// `max_age` in YAML.
    pub max_age: Duration,
    /// `max_depth` in YAML.
    pub max_depth: i64,
}

pub const DEFAULT_RESOURCE_URL_NOTIFIER_CONFIG: ResourceUrlNotifierConfig =
    ResourceUrlNotifierConfig {
        max_age: Duration(30 * SECOND.0),
        max_depth: 200,
    };

pub(super) trait Poster: Send + Sync {
    fn process(
        &self,
        queued_at: Time,
        event: livekit::WebhookEvent,
        params: Arc<ResourceUrlNotifierParams>,
        q_len: usize,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

pub(super) struct ResourceQueueInfo {
    pub(super) resource_queue: Arc<ResourceQueue>,
    pub(super) key: String,
}

// A `*resourceQueueInfo` with its `tqi`.
pub(super) type Rqi = Arc<TimeoutQueueItem<ResourceQueueInfo>>;

#[derive(Clone, Default)]
pub struct ResourceUrlNotifierParams {
    pub http_client_params: HttpClientParams,
    pub logger: Logger,
    pub timeout: Duration,
    pub config: ResourceUrlNotifierConfig,
    pub url: String,
    pub api_key: String,
    pub api_secret: String,
    pub fields_hook: Option<FieldsHook>,
    pub event_key: Option<EventKeyFn>,
    pub filter_params: FilterParams,
}

// ResourceURLNotifier is a QueuedNotifier that sends a POST request to a Webhook URL.
// It queues up events per resource (could be egress, ingress, room, participant, track, etc.)
// to avoid blocking events of one resource blocking another resource's event(s).
// It will retry on failure, and will drop events if notification fall too far behind,
// either in age or queue depth.
pub struct ResourceUrlNotifier {
    mu: RwLock<ResourceUrlNotifierInner>,
    client: retryablehttp::Client,

    resource_queue_timeout_queue: TimeoutQueue<ResourceQueueInfo>,

    closed: CancellationToken,

    // what the sweeper reads of the parameters
    timeout: Duration,
    weak_self: Weak<ResourceUrlNotifier>,
}

pub(super) struct ResourceUrlNotifierInner {
    pub(super) params: ResourceUrlNotifierParams,
    processed_hook: Option<ProcessedHook>,

    pub(super) resource_queues: HashMap<String, Rqi>,

    filter: Filter,
}

impl ResourceUrlNotifier {
    pub fn new(mut params: ResourceUrlNotifierParams) -> Arc<ResourceUrlNotifier> {
        // a nil Logger: see the header

        if params.timeout == Duration::ZERO {
            params.timeout = DEFAULT_TIMEOUT;
        }
        if params.config.max_age == Duration::ZERO {
            params.config.max_age = DEFAULT_RESOURCE_URL_NOTIFIER_CONFIG.max_age;
        }
        if params.config.max_depth == 0 {
            params.config.max_depth = DEFAULT_RESOURCE_URL_NOTIFIER_CONFIG.max_depth;
        }
        if params.http_client_params.user_agent.is_empty() {
            params.http_client_params.user_agent = DEFAULT_USER_AGENT.to_string();
        }

        let mut rhc = retryablehttp::new_client();
        if params.http_client_params.retry_wait_min > Duration::ZERO {
            rhc.retry_wait_min = params.http_client_params.retry_wait_min;
        }
        if params.http_client_params.retry_wait_max > Duration::ZERO {
            rhc.retry_wait_max = params.http_client_params.retry_wait_max;
        }
        if params.http_client_params.max_retries > 0 {
            rhc.retry_max = params.http_client_params.max_retries;
        }
        if params.http_client_params.client_timeout > Duration::ZERO {
            rhc.http_client.timeout = params.http_client_params.client_timeout;
        }
        // rhc.Logger = &logAdapter{}: see the header of notifier.rs
        if params.http_client_params.force_ipv4 {
            rhc.http_client.force_ipv4 = true;
        }
        let timeout = params.timeout;
        let filter = new_filter(params.filter_params.clone());
        let r = Arc::new_cyclic(|weak_self| ResourceUrlNotifier {
            mu: RwLock::new(ResourceUrlNotifierInner {
                params,
                processed_hook: None,
                resource_queues: HashMap::new(),
                filter,
            }),
            client: rhc,
            resource_queue_timeout_queue: TimeoutQueue::default(),
            closed: CancellationToken::new(),
            timeout,
            weak_self: weak_self.clone(),
        });

        let sweeper = r.clone();
        time::spawn(async move { sweeper.sweeper().await });
        r
    }

    pub fn is_allowed(&self, event: &str) -> bool {
        self.mu.read().filter.is_allowed(event)
    }

    fn get_processed_hook(&self) -> Option<ProcessedHook> {
        let r = self.mu.read();
        r.processed_hook.clone()
    }

    // For the tests: the lock and what it guards.
    #[cfg(test)]
    pub(super) fn inner(&self) -> &RwLock<ResourceUrlNotifierInner> {
        &self.mu
    }

    pub(super) async fn send(
        &self,
        event: &livekit::WebhookEvent,
        params: &ResourceUrlNotifierParams,
    ) -> Result<Response, Error> {
        let encoded = serde_json::to_vec(event).map_err(|err| Error::ProtoJson(err.to_string()))?;
        // sign payload
        let sum = Sha256::digest(&encoded);
        let b64 = base64::engine::general_purpose::STANDARD.encode(sum);

        let api_key = &params.api_key;
        let api_secret = &params.api_secret;

        let at = AccessToken::new(api_key, api_secret)
            .set_valid_for(5 * MINUTE)
            .set_sha256(&b64);
        let token = at.to_jwt()?;
        // an error here is returned, "ignore and continue" upstream
        let mut req = retryablehttp::new_request("POST", &params.url, Bytes::from(encoded))?;
        req.header_set(AUTH_HEADER, &token)?;
        req.header_set(CONTENT_TYPE_HEADER, CONTENT_TYPE)?;
        req.header_set(USER_AGENT_HEADER, &params.http_client_params.user_agent)?;
        let res = self.client.r#do(&req).await?;
        Ok(res)
    }

    async fn sweeper(&self) {
        let period = (self.timeout / 2).to_std();
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = self.closed.cancelled() => {
                    return;
                }

                _ = ticker.tick() => {
                    let mut it = self.resource_queue_timeout_queue.iterate_remove_after(self.timeout);
                    while it.next() {
                        let Some(rqi) = it.item().cloned() else { break };

                        let mut r = self.mu.write();
                        if r.resource_queues.get(&rqi.value.key).is_some_and(|q| Arc::ptr_eq(q, &rqi)) {
                            r.resource_queues.remove(&rqi.value.key);
                        }
                        drop(r);

                        rqi.value.resource_queue.stop(false);
                    }
                }
            }
        }
    }
}

impl QueuedNotifier for ResourceUrlNotifier {
    fn set_keys(&self, api_key: &str, api_secret: &str) {
        let mut r = self.mu.write();

        r.params.api_key = api_key.to_string();
        r.params.api_secret = api_secret.to_string();
    }

    fn set_filter(&self, params: FilterParams) {
        let mut r = self.mu.write();

        r.filter.set_filter(params);
    }

    fn register_processed_hook(&self, hook: Arc<dyn Fn(&livekit::WebhookInfo) + Send + Sync>) {
        let mut r = self.mu.write();
        r.processed_hook = Some(hook);
    }

    fn queue_notify(
        &self,
        event: &livekit::WebhookEvent,
        opts: &[NotifyOption],
    ) -> Result<(), Error> {
        if !self.is_allowed(&event.event) {
            return Ok(());
        }

        if self.closed.is_cancelled() {
            return Err(Error::Closed);
        }

        let event_key_fn = self.mu.read().params.event_key.clone();
        let key = match event_key_fn {
            Some(f) => f(event),
            None => event_key(event),
        };

        let mut p = NotifyParams::default();
        for o in opts {
            o(&mut p);
        }

        let r = self.mu.write();
        // copy the parameters
        let mut params = r.params.clone();
        drop(r);

        if p.extra_webhooks.len() > 1 {
            return Err(Error::MoreThanOneExtraWebhook);
        }
        if p.extra_webhooks.len() == 1 {
            params.url = p.extra_webhooks[0].url.clone();
            if !p.extra_webhooks[0].signing_key.is_empty() {
                params.api_key = p.extra_webhooks[0].signing_key.clone();
            }
        }

        if !p.secret.is_empty() {
            params.api_secret = p.secret.clone();
        }

        if params.api_key.is_empty() || params.api_secret.is_empty() {
            return Err(Error::NoKey);
        }
        let params = Arc::new(params);

        let mut r = self.mu.write();
        let rqi = match r.resource_queues.get(&key) {
            Some(rqi) if self.resource_queue_timeout_queue.reset(rqi) => rqi.clone(),
            _ => {
                let poster: Arc<dyn Poster> = self
                    .weak_self
                    .upgrade()
                    .expect("a notifier that is called is alive");
                let rq = new_resource_queue(ResourceQueueParams {
                    max_depth: params.config.max_depth,
                    poster,
                });
                let rqi = TimeoutQueueItem::new(ResourceQueueInfo {
                    resource_queue: rq,
                    key: key.clone(),
                });
                self.resource_queue_timeout_queue.reset(&rqi);
                r.resource_queues.insert(key, rqi.clone());
                rqi
            }
        };
        drop(r);

        match rqi.value.resource_queue.enqueue(event, &params) {
            Err(err) => {
                let mut fields = log_fields(event, &params.url);
                fields.push(("reason", Box::new(err.to_string())));
                params.logger.infow("dropped webhook", &log_kvs(&fields));
                inc_dispatch_drop(&err.to_string());

                if let Some(ph) = self.get_processed_hook() {
                    let mut whi = webhook_info(
                        event,
                        Time::ZERO,
                        Duration::ZERO,
                        Time::ZERO,
                        Duration::ZERO,
                        &params.url,
                        true,
                        None,
                    );
                    if let Some(fields_hook) = &params.fields_hook {
                        fields_hook(&mut whi);
                    }
                    ph(&whi);
                }
                Err(err)
            }
            Ok(q_len) => {
                record_queue_length(q_len);
                Ok(())
            }
        }
    }

    fn stop(&self, force: bool) {
        self.closed.cancel();

        let mut r = self.mu.write();
        let resource_queues = std::mem::take(&mut r.resource_queues);
        drop(r);

        for rq in resource_queues.values() {
            rq.value.resource_queue.stop(force);
        }
    }
}

// poster interface
impl Poster for ResourceUrlNotifier {
    fn process(
        &self,
        queued_at: Time,
        event: livekit::WebhookEvent,
        params: Arc<ResourceUrlNotifierParams>,
        q_len: usize,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let mut fields = log_fields(&event, &params.url);

            let queue_duration = time::since(queued_at);
            fields.push(("queueDuration", Box::new(queue_duration)));
            fields.push(("qLen", Box::new(q_len)));

            if queue_duration > params.config.max_age {
                fields.push(("reason", Box::new("age")));
                params.logger.infow("dropped webhook", &log_kvs(&fields));
                inc_dispatch_drop("age");

                if let Some(ph) = self.get_processed_hook() {
                    let mut whi = webhook_info(
                        &event,
                        queued_at,
                        queue_duration,
                        Time::ZERO,
                        Duration::ZERO,
                        &params.url,
                        true,
                        None,
                    );
                    if let Some(fields_hook) = &params.fields_hook {
                        fields_hook(&mut whi);
                    }
                    ph(&whi);
                }
                return;
            }

            let send_start = time::now();
            let res = self.send(&event, &params).await;
            let send_duration = time::since(send_start);
            fields.push(("sendDuration", Box::new(send_duration)));
            if let Ok(res) = &res {
                fields.push(("status", Box::new(res.status.clone())));
                fields.push(("statusCode", Box::new(res.status_code)));
            }
            match &res {
                Err(err) => {
                    params
                        .logger
                        .warnw("failed to send webhook", Some(err), &log_kvs(&fields));
                    inc_dispatch_failure();
                }
                Ok(_) => {
                    params.logger.infow("sent webhook", &log_kvs(&fields));
                    inc_dispatch_success();
                }
            }
            if let Some(ph) = self.get_processed_hook() {
                let mut whi = webhook_info(
                    &event,
                    queued_at,
                    queue_duration,
                    send_start,
                    send_duration,
                    &params.url,
                    false,
                    res.as_ref().err(),
                );
                if let Some(fields_hook) = &params.fields_hook {
                    fields_hook(&mut whi);
                }
                ph(&whi);
            }
        })
    }
}
