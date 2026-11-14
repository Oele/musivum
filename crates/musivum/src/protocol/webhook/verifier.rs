// Ported from LiveKit's protocol, webhook/verifier.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. A request is an `http::Request` whose body has been
// read already, whatever server read it: it is borrowed, and there is nothing to close or to
// fail reading. `subtle.ConstantTimeCompare` is written out. The protocol's JSON decoder here
// (pbjson) refuses a field it does not know, where the original's options discard it: an event
// of a newer LiveKit with fields this version does not have is an error here.

use base64::Engine as _;
use sha2::{Digest, Sha256};

use super::{AUTH_HEADER, Error};
use crate::protocol::auth::{self, KeyProvider};
use crate::protocol::livekit;

// Receive reads and verifies incoming webhook is signed with key/secret pair
// closes body after reading
pub fn receive<B: AsRef<[u8]>>(
    r: &http::Request<B>,
    provider: &dyn KeyProvider,
) -> Result<Vec<u8>, Error> {
    let data = r.body().as_ref().to_vec();

    let auth_token = r
        .headers()
        .get(AUTH_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if auth_token.is_empty() {
        return Err(Error::NoAuthHeader);
    }

    let v = auth::parse_api_token(auth_token)?;

    let secret = provider.get_secret(v.api_key());
    if secret.is_empty() {
        return Err(Error::SecretNotFound);
    }

    let (_, claims) = v.verify(secret.as_bytes())?;

    // verify checksum
    let sha = Sha256::digest(&data);
    let hash = base64::engine::general_purpose::STANDARD.encode(sha);

    if !constant_time_compare(claims.sha256.as_bytes(), hash.as_bytes()) {
        return Err(Error::InvalidChecksum);
    }

    Ok(data)
}

// ReceiveWebhookEvent reads and verifies incoming webhook, and returns a parsed WebhookEvent
pub fn receive_webhook_event<B: AsRef<[u8]>>(
    r: &http::Request<B>,
    provider: &dyn KeyProvider,
) -> Result<livekit::WebhookEvent, Error> {
    let data = receive(r, provider)?;
    let event: livekit::WebhookEvent =
        serde_json::from_slice(&data).map_err(|err| Error::ProtoJson(err.to_string()))?;
    Ok(event)
}

// `subtle.ConstantTimeCompare(x, y) == 1`: slices of different lengths differ at once, the
// contents are compared in a time that does not depend on them.
fn constant_time_compare(x: &[u8], y: &[u8]) -> bool {
    if x.len() != y.len() {
        return false;
    }
    x.iter().zip(y).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}
