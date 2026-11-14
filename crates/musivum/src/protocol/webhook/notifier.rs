// Ported from LiveKit's protocol, webhook/notifier.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The `context.Context` of `QueueNotify` and of the
// processed hook is dropped, as everywhere in musivum. A `NotifyOption` is a boxed closure and
// the options are a slice. The processed hook is an `Arc`, since `DefaultNotifier` hands the
// one it is given to each of its notifiers. `NewDefaultNotifier` is `new_default_notifier` and
// returns the notifier as the interface, as the original does. `Stop` stops the notifiers on
// threads of their own and waits for them, as the original does with goroutines. The embedded
// `HTTPClientParams` and `FilterParams` of the notifiers' parameters are fields named after
// them. `logAdapter`, which silences the logger of the HTTP library, has no counterpart: the
// client here (`retryablehttp`) logs nothing. `logFields` gives the keys and values as a vector
// that `log_kvs` hands to the logger. `webhookInfo` makes the zero time the Unix epoch, where
// the original's `timestamppb.New(time.Time{})` is the year 1 (it is in what the processed hook
// gets for a dropped event, never in a webhook).

use std::fmt;
use std::sync::Arc;

use super::Error;
use super::resource_url_notifier::{
    DEFAULT_RESOURCE_URL_NOTIFIER_CONFIG, ResourceUrlNotifier, ResourceUrlNotifierConfig,
    ResourceUrlNotifierParams,
};
use super::url_notifier::{DEFAULT_URL_NOTIFIER_CONFIG, UrlNotifierConfig};
use crate::go::time::{Duration, Time, timestamppb_new};
use crate::protocol::auth::KeyProvider;
use crate::protocol::livekit;
use crate::protocol::logger;
use crate::warnw;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WebHookConfig {
    /// `urls` in YAML.
    pub urls: Vec<String>,
    /// `api_key` in YAML.
    pub api_key: String,
    /// `url_notifier` in YAML.
    pub url_notifier: UrlNotifierConfig,
    /// `resource_url_notifier` in YAML.
    pub resource_url_notifier: ResourceUrlNotifierConfig,
    /// `filter_params` in YAML.
    pub filter_params: FilterParams,
}

pub const DEFAULT_WEB_HOOK_CONFIG: WebHookConfig = WebHookConfig {
    urls: Vec::new(),
    api_key: String::new(),
    url_notifier: DEFAULT_URL_NOTIFIER_CONFIG,
    resource_url_notifier: DEFAULT_RESOURCE_URL_NOTIFIER_CONFIG,
    filter_params: FilterParams {
        include_events: Vec::new(),
        exclude_events: Vec::new(),
    },
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NotifyParams {
    pub extra_webhooks: Vec<livekit::WebhookConfig>,
    pub secret: String,
}

pub type NotifyOption = Box<dyn Fn(&mut NotifyParams) + Send + Sync>;

/// The processed hook, `func(ctx context.Context, whi *livekit.WebhookInfo)` upstream.
pub type ProcessedHook = Arc<dyn Fn(&livekit::WebhookInfo) + Send + Sync>;
/// `FieldsHook` of the notifiers' parameters.
pub type FieldsHook = Arc<dyn Fn(&mut livekit::WebhookInfo) + Send + Sync>;
/// `EventKey` of the notifiers' parameters.
pub type EventKeyFn = Arc<dyn Fn(&livekit::WebhookEvent) -> String + Send + Sync>;

pub fn with_extra_webhooks(wh: Vec<livekit::WebhookConfig>) -> NotifyOption {
    Box::new(move |p: &mut NotifyParams| {
        p.extra_webhooks = wh.clone();
    })
}

pub fn with_secret(secret: &str) -> NotifyOption {
    let secret = secret.to_string();
    Box::new(move |p: &mut NotifyParams| {
        p.secret = secret.clone();
    })
}

pub trait QueuedNotifier: Send + Sync {
    fn register_processed_hook(&self, f: Arc<dyn Fn(&livekit::WebhookInfo) + Send + Sync>);
    fn set_keys(&self, api_key: &str, api_secret: &str);
    fn set_filter(&self, params: FilterParams);
    fn queue_notify(
        &self,
        event: &livekit::WebhookEvent,
        opts: &[NotifyOption],
    ) -> Result<(), Error>;
    fn stop(&self, force: bool);
}

pub struct DefaultNotifier {
    kp: Arc<dyn KeyProvider>,

    notifiers: Vec<Arc<dyn QueuedNotifier>>,
    extra_webhook_notifier: Arc<dyn QueuedNotifier>,
}

pub fn new_default_notifier(
    config: &WebHookConfig,
    kp: Arc<dyn KeyProvider>,
) -> Result<Arc<dyn QueuedNotifier>, Error> {
    let api_secret = kp.get_secret(&config.api_key);
    if api_secret.is_empty() && !config.urls.is_empty() {
        return Err(Error::UnknownApiKey);
    }

    let mut notifiers: Vec<Arc<dyn QueuedNotifier>> = Vec::new();
    for url in &config.urls {
        let u = ResourceUrlNotifier::new(ResourceUrlNotifierParams {
            url: url.clone(),
            logger: logger::get_logger().with_component("webhook"),
            api_key: config.api_key.clone(),
            api_secret: api_secret.clone(),
            config: config.resource_url_notifier,
            filter_params: config.filter_params.clone(),
            ..Default::default()
        });
        notifiers.push(u);
    }

    let extra_webhook_notifier = ResourceUrlNotifier::new(ResourceUrlNotifierParams {
        logger: logger::get_logger().with_component("webhook"),
        api_key: config.api_key.clone(),
        api_secret,
        config: config.resource_url_notifier,
        ..Default::default()
    });

    Ok(Arc::new(DefaultNotifier {
        kp,
        notifiers,
        extra_webhook_notifier,
    }))
}

impl QueuedNotifier for DefaultNotifier {
    fn stop(&self, force: bool) {
        crate::go::core::block_in_place(|| {
            std::thread::scope(|wg| {
                for u in &self.notifiers {
                    wg.spawn(move || {
                        u.stop(force);
                    });
                }

                wg.spawn(|| {
                    self.extra_webhook_notifier.stop(force);
                });
            });
        });
    }

    fn queue_notify(
        &self,
        event: &livekit::WebhookEvent,
        opts: &[NotifyOption],
    ) -> Result<(), Error> {
        for u in &self.notifiers {
            // No override for static notifiers
            u.queue_notify(event, &[])?;
        }

        let mut p = NotifyParams::default();
        for o in opts {
            o(&mut p);
        }

        for wh in &p.extra_webhooks {
            let mut lopts: Vec<NotifyOption> = vec![with_extra_webhooks(vec![wh.clone()])];

            if !wh.signing_key.is_empty() {
                // empty signing key means default
                let k = self.kp.get_secret(&wh.signing_key);
                if k.is_empty() {
                    return Err(Error::NoSecretForSigningKey);
                }

                lopts.push(with_secret(&k));
            }

            self.extra_webhook_notifier.queue_notify(event, &lopts)?;
        }

        Ok(())
    }

    fn register_processed_hook(&self, hook: Arc<dyn Fn(&livekit::WebhookInfo) + Send + Sync>) {
        for u in &self.notifiers {
            u.register_processed_hook(hook.clone());
        }
    }

    fn set_keys(&self, api_key: &str, api_secret: &str) {
        for u in &self.notifiers {
            u.set_keys(api_key, api_secret);
        }
    }

    fn set_filter(&self, params: FilterParams) {
        for u in &self.notifiers {
            u.set_filter(params.clone());
        }
    }
}

// ---------------------------------

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HttpClientParams {
    pub retry_wait_min: Duration,
    pub retry_wait_max: Duration,
    pub max_retries: i64,
    pub client_timeout: Duration,
    pub force_ipv4: bool,
    // receivers pin WAF rules to this, so it must stay stable across releases
    // defaults to DefaultUserAgent
    pub user_agent: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FilterParams {
    /// `include_events` in YAML.
    pub include_events: Vec<String>,
    /// `exclude_events` in YAML.
    pub exclude_events: Vec<String>,
}

// ---------------------------------

// Not here: logAdapter, see the header.

// ---------------------------------

pub fn event_key(event: &livekit::WebhookEvent) -> String {
    if let Some(egress_info) = &event.egress_info {
        return egress_info.egress_id.clone();
    }
    if let Some(ingress_info) = &event.ingress_info {
        return ingress_info.ingress_id.clone();
    }
    if let Some(room) = &event.room {
        return room.name.clone();
    }
    if let Some(participant) = &event.participant {
        return participant.identity.clone();
    }
    if let Some(track) = &event.track {
        return track.sid.clone();
    }
    warnw!(logger::get_logger(), "webhook using default event", nil, "event" => event);
    "default".to_string()
}

/// The keys and values `logFields` gives, for `log_kvs`.
pub(super) type LogFields = Vec<(&'static str, Box<dyn fmt::Debug + Send + Sync>)>;

/// The fields as the logger takes them.
pub(super) fn log_kvs(fields: &LogFields) -> Vec<(&str, &dyn fmt::Debug)> {
    fields
        .iter()
        .map(|(k, v)| (*k, v.as_ref() as &dyn fmt::Debug))
        .collect()
}

pub(super) fn log_fields(event: &livekit::WebhookEvent, url: &str) -> LogFields {
    let mut fields: LogFields = Vec::with_capacity(20);
    fields.push(("event", Box::new(event.event.clone())));
    fields.push(("id", Box::new(event.id.clone())));
    fields.push(("webhookTime", Box::new(event.created_at)));
    fields.push(("url", Box::new(url.to_string())));

    if let Some(room) = &event.room {
        fields.push(("room", Box::new(room.name.clone())));
        fields.push(("roomID", Box::new(room.sid.clone())));
    }
    if let Some(participant) = &event.participant {
        fields.push(("participant", Box::new(participant.identity.clone())));
        fields.push(("participantID", Box::new(participant.sid.clone())));
    }
    if let Some(track) = &event.track {
        fields.push(("trackID", Box::new(track.sid.clone())));
    }
    if let Some(egress_info) = &event.egress_info {
        fields.push(("egressID", Box::new(egress_info.egress_id.clone())));
        fields.push(("status", Box::new(egress_info.status().as_str_name())));
        if !egress_info.error.is_empty() {
            fields.push(("error", Box::new(egress_info.error.clone())));
        }
    }
    if let Some(ingress_info) = &event.ingress_info {
        fields.push(("ingressID", Box::new(ingress_info.ingress_id.clone())));
        if let Some(state) = &ingress_info.state {
            fields.push(("status", Box::new(state.status().as_str_name())));
            if !state.error.is_empty() {
                fields.push(("error", Box::new(state.error.clone())));
            }
        }
    }
    fields
}

#[allow(clippy::too_many_arguments)]
pub(super) fn webhook_info(
    event: &livekit::WebhookEvent,
    queued_at: Time,
    queue_duration: Duration,
    sent_at: Time,
    send_duration: Duration,
    url: &str,
    is_dropped: bool,
    send_error: Option<&Error>,
) -> livekit::WebhookInfo {
    let mut whi = livekit::WebhookInfo {
        event_id: event.id.clone(),
        event: event.event.clone(),
        created_at: Some(timestamppb_new(Time::unix(event.created_at, 0))),
        queued_at: Some(timestamppb_new(queued_at)),
        queue_duration_ns: queue_duration.nanoseconds(),
        sent_at: Some(timestamppb_new(sent_at)),
        send_duration_ns: send_duration.nanoseconds(),
        url: url.to_string(),
        num_dropped: event.num_dropped,
        is_dropped,
        ..Default::default()
    };
    if !queued_at.is_zero() {
        whi.queued_at = Some(timestamppb_new(queued_at));
    }
    if !sent_at.is_zero() {
        whi.sent_at = Some(timestamppb_new(sent_at));
    }
    if let Some(room) = &event.room {
        whi.room_name = room.name.clone();
        whi.room_id = room.sid.clone();
    }
    if let Some(participant) = &event.participant {
        whi.participant_identity = participant.identity.clone();
        whi.participant_id = participant.sid.clone();
    }
    if let Some(track) = &event.track {
        whi.track_id = track.sid.clone();
    }
    if let Some(egress_info) = &event.egress_info {
        whi.egress_id = egress_info.egress_id.clone();
        whi.service_status = egress_info.status().as_str_name().to_string();
        if !egress_info.error.is_empty() {
            whi.service_error_code = egress_info.error_code;
            whi.service_error = egress_info.error.clone();
        }
    }
    if let Some(ingress_info) = &event.ingress_info {
        whi.ingress_id = ingress_info.ingress_id.clone();
        if let Some(state) = &ingress_info.state {
            whi.service_status = state.status().as_str_name().to_string();
            if !state.error.is_empty() {
                whi.service_error = state.error.clone();
            }
        }
    }
    if let Some(send_error) = send_error {
        whi.send_error = send_error.to_string();
    }
    whi
}
