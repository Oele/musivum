// Ported from LiveKit's protocol, webhook/url_notifier.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The `context.Context` of `QueueNotify` and of the
// processed hook is dropped. `NewURLNotifier` returns an `Arc`: a job, a future that the pool
// (`pool.rs`, frostbyte73/core's `QueuePool`) awaits, keeps the notifier, as the original's
// closure does. The event is copied into the job, and `send` sets the dropped count in that
// copy, where the original sets it in the caller's event. The embedded `HTTPClientParams` and
// `FilterParams` are fields named after them; a nil `Logger` has no counterpart (the default
// is `logger::get_logger()`); `FieldsHook` and `EventKey` are optional closures in `Arc`s.
// The filter and the event key are read under the lock that guards their changes, where the
// original reads them without. `send` is a future, the HTTP client is `retryablehttp.rs`.

use std::sync::{Arc, Weak};

use base64::Engine as _;
use bytes::Bytes;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};

use super::filter::{Filter, new_filter};
use super::notifier::{
    EventKeyFn, FieldsHook, FilterParams, HttpClientParams, NotifyOption, NotifyParams,
    ProcessedHook, QueuedNotifier, event_key, log_fields, log_kvs, webhook_info,
};
use super::pool::{QueuePool, QueueWorkerParams, new_queue_pool};
use super::retryablehttp::{self, Response};
use super::stats::{inc_dispatch_drop, inc_dispatch_failure, inc_dispatch_success};
use super::{
    AUTH_HEADER, CONTENT_TYPE, CONTENT_TYPE_HEADER, DEFAULT_USER_AGENT, Error, USER_AGENT_HEADER,
};
use crate::go::atomic;
use crate::go::time::{self, Duration, MINUTE, Time};
use crate::protocol::auth::AccessToken;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UrlNotifierConfig {
    /// `num_workers` in YAML.
    pub num_workers: i64,
    /// `queue_size` in YAML.
    pub queue_size: i64,
}

pub const DEFAULT_URL_NOTIFIER_CONFIG: UrlNotifierConfig = UrlNotifierConfig {
    num_workers: 10,
    queue_size: 100,
};

#[derive(Clone, Default)]
pub struct UrlNotifierParams {
    pub http_client_params: HttpClientParams,
    pub logger: Logger,
    pub config: UrlNotifierConfig,
    pub url: String,
    pub api_key: String,
    pub api_secret: String,
    pub fields_hook: Option<FieldsHook>,
    pub event_key: Option<EventKeyFn>,
    pub filter_params: FilterParams,
}

// URLNotifier is a QueuedNotifier that sends a POST request to a Webhook URL.
// It will retry on failure, and will drop events if notification fall too far behind
pub struct UrlNotifier {
    mu: RwLock<UrlNotifierInner>,
    client: retryablehttp::Client,
    dropped: atomic::Int32,
    pool: QueuePool,

    weak_self: Weak<UrlNotifier>,
}

pub(super) struct UrlNotifierInner {
    pub(super) params: UrlNotifierParams,
    processed_hook: Option<ProcessedHook>,
    filter: Filter,
}

impl UrlNotifier {
    pub fn new(mut params: UrlNotifierParams) -> Arc<UrlNotifier> {
        if params.config.num_workers == 0 {
            params.config.num_workers = DEFAULT_URL_NOTIFIER_CONFIG.num_workers;
        }
        if params.config.queue_size == 0 {
            params.config.queue_size = DEFAULT_URL_NOTIFIER_CONFIG.queue_size;
        }
        // a nil Logger: see the header
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
        if params.http_client_params.force_ipv4 {
            rhc.http_client.force_ipv4 = true;
        }
        let filter = new_filter(params.filter_params.clone());
        // n.client.Logger = &logAdapter{}: see the header of notifier.rs

        let pool = new_queue_pool(
            params.config.num_workers,
            QueueWorkerParams {
                queue_size: params.config.queue_size,
                drop_when_full: true,
            },
        );
        Arc::new_cyclic(|weak_self| UrlNotifier {
            mu: RwLock::new(UrlNotifierInner {
                params,
                processed_hook: None,
                filter,
            }),
            client: rhc,
            dropped: atomic::Int32::default(),
            pool,
            weak_self: weak_self.clone(),
        })
    }

    pub fn is_allowed(&self, event: &str) -> bool {
        self.mu.read().filter.is_allowed(event)
    }

    fn get_processed_hook(&self) -> Option<ProcessedHook> {
        let n = self.mu.read();
        n.processed_hook.clone()
    }

    // For the tests: the lock and what it guards.
    #[cfg(test)]
    pub(super) fn inner(&self) -> &RwLock<UrlNotifierInner> {
        &self.mu
    }

    pub(super) async fn send(
        &self,
        event: &mut livekit::WebhookEvent,
        params: &UrlNotifierParams,
    ) -> Result<Response, Error> {
        // set dropped count
        event.num_dropped = self.dropped.swap(0);
        let encoded = serde_json::to_vec(event).map_err(|err| Error::ProtoJson(err.to_string()))?;
        // sign payload
        let sum = Sha256::digest(&encoded);
        let b64 = base64::engine::general_purpose::STANDARD.encode(sum);

        let at = AccessToken::new(&params.api_key, &params.api_secret)
            .set_valid_for(5 * MINUTE)
            .set_sha256(&b64);
        let token = at.to_jwt()?;
        // an error here is returned, "ignore and continue" upstream
        let mut r = retryablehttp::new_request("POST", &params.url, Bytes::from(encoded))?;
        r.header_set(AUTH_HEADER, &token)?;
        r.header_set(CONTENT_TYPE_HEADER, CONTENT_TYPE)?;
        r.header_set(USER_AGENT_HEADER, &params.http_client_params.user_agent)?;
        let res = self.client.r#do(&r).await?;
        Ok(res)
    }
}

impl QueuedNotifier for UrlNotifier {
    fn set_keys(&self, api_key: &str, api_secret: &str) {
        let mut n = self.mu.write();
        n.params.api_key = api_key.to_string();
        n.params.api_secret = api_secret.to_string();
    }

    fn set_filter(&self, params: FilterParams) {
        let mut n = self.mu.write();
        n.filter.set_filter(params);
    }

    fn register_processed_hook(&self, hook: Arc<dyn Fn(&livekit::WebhookInfo) + Send + Sync>) {
        let mut n = self.mu.write();
        n.processed_hook = Some(hook);
    }

    fn queue_notify(
        &self,
        event: &livekit::WebhookEvent,
        opts: &[NotifyOption],
    ) -> Result<(), Error> {
        if !self.is_allowed(&event.event) {
            return Ok(());
        }

        let enqueued_at = time::now();

        let event_key_fn = self.mu.read().params.event_key.clone();
        let key = match event_key_fn {
            Some(f) => f(event),
            None => event_key(event),
        };

        let mut p = NotifyParams::default();
        for o in opts {
            o(&mut p);
        }

        let n = self.mu.read();
        let mut params = n.params.clone();
        drop(n);

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

        let this = self
            .weak_self
            .upgrade()
            .expect("a notifier that is called is alive");
        let mut job_event = event.clone();
        let job_params = params.clone();
        let submitted = self.pool.submit(&key, async move {
            let event = &mut job_event;
            let params = job_params;
            let mut fields = log_fields(event, &params.url);

            let queue_duration = time::since(enqueued_at);
            fields.push(("queueDuration", Box::new(queue_duration)));

            let send_start = time::now();
            let res = this.send(event, &params).await;
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
                    this.dropped.add(event.num_dropped.wrapping_add(1));
                    inc_dispatch_failure();
                }
                Ok(_) => {
                    params.logger.infow("sent webhook", &log_kvs(&fields));
                    inc_dispatch_success();
                }
            }
            if let Some(ph) = this.get_processed_hook() {
                let mut whi = webhook_info(
                    event,
                    enqueued_at,
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
        });
        if !submitted {
            self.dropped.inc();

            let fields = log_fields(event, &params.url);
            params.logger.infow("dropped webhook", &log_kvs(&fields));
            inc_dispatch_drop("overflow");

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
        }
        Ok(())
    }

    fn stop(&self, force: bool) {
        if force {
            self.pool.kill();
        } else {
            self.pool.drain();
        }
    }
}
