// Ported from LiveKit, pkg/service/requestlimit_test.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The limiter is laid over a router whose handler is
// `readAllHandler`; what the handler records is shared with the test through a lock. A body
// whose length is not known is one sent without a Content-Length header, where the original
// sets `r.ContentLength = -1`. "passes through nil body" sends an empty body (a request here
// always has one).

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header};
use axum::middleware::{Next, from_fn};
use axum::response::IntoResponse;
use parking_lot::Mutex;
use tower::ServiceExt;

use super::RequestBodyLimiter;

// readAllHandler mimics the way a Twirp handler consumes the whole request body
// before doing anything else. It records how much it managed to read and whether
// the read failed (e.g. because the body limit was exceeded).
#[derive(Default)]
struct ReadAllHandler {
    bytes_read: usize,
    read_err: Option<String>,
    called: bool,
}

async fn serve_http(h: Arc<Mutex<ReadAllHandler>>, r: Request) -> StatusCode {
    h.lock().called = true;
    match axum::body::to_bytes(r.into_body(), usize::MAX).await {
        Ok(b) => {
            h.lock().bytes_read = b.len();
            StatusCode::OK
        }
        Err(err) => {
            h.lock().read_err = Some(err.to_string());
            // a real decoder surfaces this as a 4xx/5xx; emulate that
            StatusCode::BAD_REQUEST
        }
    }
}

// `l.ServeHTTP(w, r, handler.ServeHTTP)`: the request through the limiter to the handler.
async fn serve(
    l: RequestBodyLimiter,
    handler: &Arc<Mutex<ReadAllHandler>>,
    r: Request,
) -> StatusCode {
    let handler = handler.clone();
    let app = Router::new()
        .fallback(move |r: Request| serve_http(handler.clone(), r))
        .layer(from_fn(move |r: Request, next: Next| async move {
            l.serve_http(r, next).await.into_response()
        }));
    app.oneshot(r).await.unwrap().status()
}

const LIMIT: usize = 1024;
const PATH: &str = "/twirp/livekit.Egress/StartRoomCompositeEgress";

#[tokio::test]
async fn test_request_body_limiter() {
    {
        // rejects oversized declared Content-Length before decoding
        let l = RequestBodyLimiter::new(LIMIT as i64);
        let handler = Arc::new(Mutex::new(ReadAllHandler::default()));

        let body = "a".repeat(LIMIT * 4);
        let r = Request::post(PATH)
            .header(header::CONTENT_LENGTH, body.len())
            .body(Body::from(body))
            .unwrap();

        let code = serve(l, &handler, r).await;

        assert_eq!(StatusCode::PAYLOAD_TOO_LARGE, code);
        // the body must never be handed to the decoder
        assert!(!handler.lock().called);
    }

    {
        // bounds body when Content-Length is absent/dishonest
        let l = RequestBodyLimiter::new(LIMIT as i64);
        let handler = Arc::new(Mutex::new(ReadAllHandler::default()));

        // simulate chunked encoding / unknown length
        let body = "a".repeat(LIMIT * 4);
        let r = Request::post(PATH).body(Body::from(body)).unwrap();

        serve(l, &handler, r).await;

        // the decoder was invoked but could not read more than the limit
        let h = handler.lock();
        assert!(h.called);
        assert!(h.read_err.is_some());
        assert!(h.bytes_read <= LIMIT);
    }

    {
        // allows request within limit
        let l = RequestBodyLimiter::new(LIMIT as i64);
        let handler = Arc::new(Mutex::new(ReadAllHandler::default()));

        let payload = "a".repeat(LIMIT / 2);
        let r = Request::post(PATH)
            .header(header::CONTENT_LENGTH, payload.len())
            .body(Body::from(payload.clone()))
            .unwrap();

        let code = serve(l, &handler, r).await;

        assert_eq!(StatusCode::OK, code);
        let h = handler.lock();
        assert!(h.called);
        assert_eq!(None, h.read_err);
        assert_eq!(payload.len(), h.bytes_read);
    }

    {
        // disabled when limit is non-positive
        let l = RequestBodyLimiter::new(0);
        let handler = Arc::new(Mutex::new(ReadAllHandler::default()));

        let payload = "a".repeat(LIMIT * 8);
        let r = Request::post(PATH)
            .header(header::CONTENT_LENGTH, payload.len())
            .body(Body::from(payload.clone()))
            .unwrap();

        let code = serve(l, &handler, r).await;

        assert_eq!(StatusCode::OK, code);
        let h = handler.lock();
        assert_eq!(None, h.read_err);
        assert_eq!(payload.len(), h.bytes_read);
    }

    {
        // passes through nil body
        let l = RequestBodyLimiter::new(LIMIT as i64);
        let handler = Arc::new(Mutex::new(ReadAllHandler::default()));

        let r = Request::get("/").body(Body::empty()).unwrap();

        serve(l, &handler, r).await;

        assert!(handler.lock().called);
    }
}
