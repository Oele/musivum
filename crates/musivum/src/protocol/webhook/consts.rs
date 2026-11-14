// Ported from LiveKit's protocol, webhook/consts.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The errors of the package are one enum, here: the
// exported ones of this file, the unexported ones of `resource_url_notifier.go` and
// `resource_queue.go`, the errors the original makes with `fmt.Errorf`, and what it passes on
// from the token library, the JSON encoder and the HTTP client.

use crate::protocol::auth;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("authorization header could not be found")]
    NoAuthHeader,
    #[error("API secret could not be found")]
    SecretNotFound,
    #[error("could not verify authenticity of message")]
    InvalidChecksum,
    // errClosed of resource_url_notifier.go
    #[error("notifier is closed")]
    Closed,
    // errNoKey of resource_url_notifier.go, with the original's spelling
    #[error("no singing key or secret was provided")]
    NoKey,
    // errQueueFull of resource_queue.go
    #[error("queue is full")]
    QueueFull,
    // errQueueClosed of resource_queue.go
    #[error("queue is closed")]
    QueueClosed,
    // fmt.Errorf of NewDefaultNotifier
    #[error("unknown api key in webhook config")]
    UnknownApiKey,
    // fmt.Errorf of DefaultNotifier.QueueNotify
    #[error("no secret for provided signing key")]
    NoSecretForSigningKey,
    // fmt.Errorf of the notifiers' QueueNotify
    #[error("more than 1 extra webhook url unexpected")]
    MoreThanOneExtraWebhook,
    /// What the token library says of a token it cannot make or does not take.
    #[error(transparent)]
    Auth(#[from] auth::Error),
    /// What the protocol's JSON encoder or decoder says of a message.
    #[error("{0}")]
    ProtoJson(String),
    /// What the HTTP client says: a URL it cannot send to, or a request that failed after
    /// its retries.
    #[error("{0}")]
    Http(String),
}

pub(super) const AUTH_HEADER: &str = "Authorization";
pub(super) const CONTENT_TYPE_HEADER: &str = "content-type";
pub(super) const USER_AGENT_HEADER: &str = "User-Agent";

// use a custom mime type to ensure signature is checked prior to parsing
pub const CONTENT_TYPE: &str = "application/webhook+json";
pub const DEFAULT_USER_AGENT: &str = "LiveKit";

pub const EVENT_ROOM_STARTED: &str = "room_started";
pub const EVENT_ROOM_FINISHED: &str = "room_finished";
pub const EVENT_PARTICIPANT_JOINED: &str = "participant_joined";
pub const EVENT_PARTICIPANT_LEFT: &str = "participant_left";
pub const EVENT_PARTICIPANT_CONNECTION_ABORTED: &str = "participant_connection_aborted";
pub const EVENT_TRACK_PUBLISHED: &str = "track_published";
pub const EVENT_TRACK_UNPUBLISHED: &str = "track_unpublished";
pub const EVENT_EGRESS_STARTED: &str = "egress_started";
pub const EVENT_EGRESS_UPDATED: &str = "egress_updated";
pub const EVENT_EGRESS_ENDED: &str = "egress_ended";
pub const EVENT_INGRESS_STARTED: &str = "ingress_started";
pub const EVENT_INGRESS_ENDED: &str = "ingress_ended";
pub const EVENT_AGENT_JOB_PENDING: &str = "agent_job_pending";
pub const EVENT_AGENT_JOB_STARTED: &str = "agent_job_started";
pub const EVENT_AGENT_JOB_ENDED: &str = "agent_job_ended";
