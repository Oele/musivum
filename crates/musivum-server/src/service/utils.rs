// Ported from LiveKit, pkg/service/utils.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; only the middleware of Go's net/http, which
// `musivum::service::utils` leaves to the server (the rest of the file is there). A
// middleware is an axum middleware function (`next` is axum's `Next`); `WithPathNormalization`
// is a layer laid over a whole router, so that routing sees the normalised path. Go's
// `URL.Path` and `URL.RawPath` are one path here (the `http` crate keeps a path as it came,
// escaped), so moving them together is moving that one. `RequestBodyLimiter` answers as
// `HandleError` does, with the log line of `musivum::service::handle_error` (its error type
// has no `ErrRequestBodyTooLarge`); the body it bounds is `http_body_util::Limited`, where
// the original wraps it in `http.MaxBytesReader`, and the original's branch for a nil body
// has no counterpart (a request here always has one, empty or not).

use axum::body::Body;
use axum::extract::Request;
use axum::middleware::{Next, from_fn};
use axum::response::{IntoResponse, Response};
use http_body_util::Limited;
use musivum::protocol::logger;
use musivum::warnw;
use tower::Layer;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("request body too large")]
    RequestBodyTooLarge,
}

// HandleError, with the error of this file: logs as the original does and answers with the
// status and the error's text.
fn handle_error(r: &Request, status: axum::http::StatusCode, err: &Error) -> Response {
    warnw!(
        logger::get_logger(),
        "error handling request",
        err,
        "status" => status.as_u16(),
        "method" => r.method().as_str(),
        "path" => r.uri().path(),
    );
    (status, err.to_string()).into_response()
}

pub async fn remove_double_slashes(mut r: Request, next: Next) -> Response {
    // Path and RawPath must move together: once they disagree,
    // EscapedPath() re-encodes from Path and every escape in the request is lost.
    if r.uri().path().starts_with("//") {
        let mut parts = r.uri().clone().into_parts();
        if let Some(path_and_query) = &parts.path_and_query
            && let Ok(trimmed) = path_and_query.as_str()[1..].parse()
        {
            parts.path_and_query = Some(trimmed);
            if let Ok(uri) = axum::http::Uri::from_parts(parts) {
                *r.uri_mut() = uri;
            }
        }
    }
    next.run(r).await
}

/// WithPathNormalization applies RemoveDoubleSlashes ahead of h, so routing and every
/// middleware below it see one path form.
pub fn with_path_normalization(h: axum::Router) -> axum::Router {
    axum::Router::new().fallback_service(from_fn(remove_double_slashes).layer(h))
}

/// RequestBodyLimiter bounds the size of an incoming HTTP request body so that
/// large messages cannot exhaust memory. The Twirp handlers decode the whole
/// body before any grant check runs, so the limit is applied here, up front.
///
/// It does not decode the body itself: a request whose Content-Length exceeds
/// the limit is rejected with 413, and the body is wrapped with
/// `Limited` so a missing or dishonest Content-Length is still caught
/// by the downstream decoder.
#[derive(Clone, Copy, Debug)]
pub struct RequestBodyLimiter {
    max_bytes: i64,
}

impl RequestBodyLimiter {
    pub fn new(max_bytes: i64) -> RequestBodyLimiter {
        RequestBodyLimiter { max_bytes }
    }

    pub async fn serve_http(self, r: Request, next: Next) -> Response {
        if self.max_bytes <= 0 {
            return next.run(r).await;
        }

        // reject early when the declared size already exceeds the limit
        if content_length(&r) > self.max_bytes {
            return handle_error(
                &r,
                axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                &Error::RequestBodyTooLarge,
            );
        }

        // bound the read in case Content-Length is missing or wrong
        let max_bytes = self.max_bytes as usize;
        let r = r.map(|body| Body::new(Limited::new(body, max_bytes)));
        next.run(r).await
    }
}

// Go's r.ContentLength: the declared length, -1 when it is not known.
fn content_length(r: &Request) -> i64 {
    r.headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(-1)
}

#[cfg(test)]
#[path = "utils_test.rs"]
mod utils_test;
