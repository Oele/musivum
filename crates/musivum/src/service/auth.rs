// Ported from LiveKit, pkg/service/auth.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, for any HTTP server: a request is the `http` crate's
// `request::Parts`, and what the original keeps in a request's `context.Context` (the
// grants, the API key, when the token expires) is kept in the parts' extensions. The
// middleware is a function: `ServeHTTP` either gives the request on with the grants in its
// extensions (Ok) or says what to answer (`ErrorResponse`, what `HandleError` writes); the
// headers it sets on the response go into the response headers it is handed. The checks
// (`EnsureJoinPermission` and the others) take the grants where the original takes the
// context they are in. Not here: `EnsureIngressAdminPermission`, `EnsureSIPAdminPermission`
// and `EnsureSIPCallPermission` (ingress and SIP are left out), and `twirpAuthError` (no
// Twirp). The errors of the file are in errors.rs.

use std::sync::Arc;

use http::request::Parts;
use http::{Extensions, HeaderMap, HeaderValue, StatusCode};

use super::{Error, ErrorResponse, handle_error, query_value};
use crate::go::time::Time;
use crate::protocol::auth::{self, ClaimGrants, KeyProvider};
use crate::protocol::livekit;

const AUTHORIZATION_HEADER: &str = "Authorization";
const BEARER_PREFIX: &str = "Bearer ";
const ACCESS_TOKEN_PARAM: &str = "access_token";

// What the original puts into a request's context under `grantsKey{}`.
#[derive(Clone)]
struct GrantsValue {
    claims: Option<ClaimGrants>,
    api_key: String,
    expires_at: Time,
}

// authentication middleware
pub struct ApiKeyAuthMiddleware {
    provider: Arc<dyn KeyProvider>,
}

impl ApiKeyAuthMiddleware {
    pub fn new(provider: Arc<dyn KeyProvider>) -> ApiKeyAuthMiddleware {
        ApiKeyAuthMiddleware { provider }
    }

    /// `ServeHTTP(w, r, next)`: Ok where the original calls `next` with the request, which
    /// then carries the grants of its token in `r.extensions` if it had one (no token is no
    /// grants, not an error); Err where it answers itself. `w` is the response's headers.
    pub fn serve_http(&self, w: &mut HeaderMap, r: &mut Parts) -> Result<(), ErrorResponse> {
        if r.uri.path() == "/rtc/validate" || r.uri.path() == "/rtc/v1/validate" {
            w.insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
        }

        let auth_header = header_value(&r.headers, AUTHORIZATION_HEADER);
        let auth_token = if !auth_header.is_empty() {
            if !auth_header.starts_with(BEARER_PREFIX) {
                return Err(handle_error(
                    r,
                    StatusCode::UNAUTHORIZED,
                    &Error::MissingAuthorization,
                    &[],
                ));
            }

            auth_header[BEARER_PREFIX.len()..].to_string()
        } else {
            // fall back to the query string rather than a form body, which must
            // survive intact for requests proxied further (agent HTTP endpoints).
            query_value(&r.uri, ACCESS_TOKEN_PARAM)
        };

        if !auth_token.is_empty() {
            let v = match auth::parse_api_token(&auth_token) {
                Ok(v) => v,
                Err(_) => {
                    return Err(handle_error(
                        r,
                        StatusCode::UNAUTHORIZED,
                        &Error::InvalidAuthorizationToken,
                        &[],
                    ));
                }
            };

            let secret = self.provider.get_secret(v.api_key());
            if secret.is_empty() {
                return Err(handle_error(
                    r,
                    StatusCode::UNAUTHORIZED,
                    &Error::InvalidApiKey,
                    &[("apiKey", &v.api_key())],
                ));
            }

            let (claims, grants) = match v.verify(&secret) {
                Ok(verified) => verified,
                Err(err) => {
                    return Err(handle_error(
                        r,
                        StatusCode::UNAUTHORIZED,
                        &Error::InvalidAuthorizationTokenDetail(err.to_string()),
                        &[],
                    ));
                }
            };

            let mut expires_at = Time::ZERO;
            if let Some(exp) = claims.expires_at {
                expires_at = Time::unix(exp.unix(), 0);
            }

            // set grants in context
            with_grants_expiry(&mut r.extensions, Some(grants), v.api_key(), expires_at);
        }

        Ok(())
    }
}

pub fn with_api_key(ctx: &mut Extensions, grants: Option<ClaimGrants>, api_key: &str) {
    ctx.insert(GrantsValue {
        claims: grants,
        api_key: api_key.to_string(),
        expires_at: Time::ZERO,
    });
}

pub fn get_grants(ctx: &Extensions) -> Option<&ClaimGrants> {
    ctx.get::<GrantsValue>()?.claims.as_ref()
}

pub fn get_token_expires_at(ctx: &Extensions) -> Time {
    match ctx.get::<GrantsValue>() {
        Some(v) => v.expires_at,
        None => Time::ZERO,
    }
}

pub fn get_api_key(ctx: &Extensions) -> String {
    match ctx.get::<GrantsValue>() {
        Some(v) => v.api_key.clone(),
        None => String::new(),
    }
}

pub fn with_grants(ctx: &mut Extensions, grants: Option<ClaimGrants>, api_key: &str) {
    with_grants_expiry(ctx, grants, api_key, Time::ZERO);
}

pub fn with_grants_expiry(
    ctx: &mut Extensions,
    grants: Option<ClaimGrants>,
    api_key: &str,
    expires_at: Time,
) {
    ctx.insert(GrantsValue {
        claims: grants,
        api_key: api_key.to_string(),
        expires_at,
    });
}

pub fn set_authorization_token(r: &mut Parts, token: &str) {
    match HeaderValue::from_str(&format!("{BEARER_PREFIX}{token}")) {
        Ok(value) => {
            r.headers.insert(AUTHORIZATION_HEADER, value);
        }
        // Go would set it as it is; a header value cannot hold such a token here
        Err(_) => {
            r.headers.remove(AUTHORIZATION_HEADER);
        }
    }
}

pub fn ensure_join_permission(claims: Option<&ClaimGrants>) -> Result<livekit::RoomName, Error> {
    let Some(video) = claims.and_then(|c| c.video.as_ref()) else {
        return Err(Error::PermissionDenied);
    };

    if video.room_join {
        Ok(livekit::RoomName::from(video.room.as_str()))
    } else {
        Err(Error::PermissionDenied)
    }
}

pub fn ensure_admin_permission(
    claims: Option<&ClaimGrants>,
    room: &livekit::RoomName,
) -> Result<(), Error> {
    let Some(video) = claims.and_then(|c| c.video.as_ref()) else {
        return Err(Error::PermissionDenied);
    };

    if !video.room_admin || room.as_str() != video.room {
        return Err(Error::PermissionDenied);
    }

    Ok(())
}

pub fn ensure_create_permission(claims: Option<&ClaimGrants>) -> Result<(), Error> {
    match claims.and_then(|c| c.video.as_ref()) {
        Some(video) if video.room_create => Ok(()),
        _ => Err(Error::PermissionDenied),
    }
}

pub fn ensure_list_permission(claims: Option<&ClaimGrants>) -> Result<(), Error> {
    match claims.and_then(|c| c.video.as_ref()) {
        Some(video) if video.room_list => Ok(()),
        _ => Err(Error::PermissionDenied),
    }
}

pub fn ensure_record_permission(claims: Option<&ClaimGrants>) -> Result<(), Error> {
    match claims.and_then(|c| c.video.as_ref()) {
        Some(video) if video.room_record => Ok(()),
        _ => Err(Error::PermissionDenied),
    }
}

// EnsureIngressAdminPermission, EnsureSIPAdminPermission and EnsureSIPCallPermission: not
// ported (ingress and SIP are left out).

pub fn ensure_dest_room_permission(
    claims: Option<&ClaimGrants>,
    source: &livekit::RoomName,
    destination: &livekit::RoomName,
) -> Result<(), Error> {
    let Some(video) = claims.and_then(|c| c.video.as_ref()) else {
        return Err(Error::PermissionDenied);
    };

    if !video.room_admin
        || source.as_str() != video.room
        || destination.as_str() != video.destination_room
    {
        return Err(Error::PermissionDenied);
    }

    Ok(())
}

// twirpAuthError: no counterpart (Twirp is not used).

// Go's `r.Header.Get(key)`: the first value, or nothing.
pub(super) fn header_value(headers: &HeaderMap, key: &str) -> String {
    headers
        .get(key)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default()
}

#[cfg(test)]
#[path = "auth_test.rs"]
mod auth_test;
