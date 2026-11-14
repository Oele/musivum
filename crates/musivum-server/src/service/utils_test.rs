// Ported from LiveKit, pkg/service/utils_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; only `TestRemoveDoubleSlashes` (the other tests of
// the file are in `musivum::service`). The request goes through the middleware to a handler
// that answers with the path it sees, where the original records `r.URL.EscapedPath()`.

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::http::Uri;
use tower::ServiceExt;

use super::*;

// URL.Path and URL.RawPath must stay in step: once they disagree,
// EscapedPath() re-encodes from Path and every escape is lost.
#[tokio::test]
async fn test_remove_double_slashes() {
    let cases = [
        (
            "doubled slash is trimmed",
            "//agents/a/d/x",
            "/agents/a/d/x",
        ),
        ("single slash untouched", "/agents/a/d/x", "/agents/a/d/x"),
        (
            "doubled slash keeps the encoded tail",
            "//agents/a/d/f%2Fg",
            "/agents/a/d/f%2Fg",
        ),
        (
            "encoded name survives",
            "//agents/LODHA%20Vayam/d/x",
            "/agents/LODHA%20Vayam/d/x",
        ),
        (
            "leading encoded slash is not doubled",
            "/%2Fagents/a/d/x",
            "/%2Fagents/a/d/x",
        ),
    ];

    for (name, target, want) in cases {
        let handler = with_path_normalization(
            Router::new().fallback(|uri: Uri| async move { uri.path().to_string() }),
        );
        let r = Request::get(target).body(Body::empty()).unwrap();
        let res = handler.oneshot(r).await.unwrap();
        let got = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(want.as_bytes(), &got[..], "{name}");
    }
}
