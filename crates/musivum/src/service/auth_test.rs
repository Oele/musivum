// Ported from LiveKit, pkg/service/auth_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The fake key provider of the original (`authfakes`,
// not ported) is `AnyKeyProvider` below, which gives the secret for every key as the fake
// is told to; the handler after the middleware is what follows an `Ok`. The test at the end
// is new.

use std::sync::Arc;

use http::{HeaderMap, Request, StatusCode};

use super::*;
use crate::protocol::auth::{AccessToken, KeyProvider, VideoGrant};

struct AnyKeyProvider(String);

impl KeyProvider for AnyKeyProvider {
    fn get_secret(&self, _key: &str) -> String {
        self.0.clone()
    }

    fn num_keys(&self) -> usize {
        1
    }
}

fn new_request(uri: &str) -> Parts {
    Request::get(uri).body(()).unwrap().into_parts().0
}

#[test]
fn test_auth_middleware() {
    let api = "APIabcdefg";
    let secret = "somesecretencodedinbase62extendto32bytes";
    let provider = Arc::new(AnyKeyProvider(secret.to_string()));

    let m = ApiKeyAuthMiddleware::new(provider);
    // the handler: the grants the request carries, and 200
    let handler = |r: &Parts| get_grants(&r.extensions).cloned();

    let orig = VideoGrant {
        room: "abcdefg".into(),
        room_join: true,
        ..Default::default()
    };
    // ensure that the original claim could be retrieved
    let at = AccessToken::new(api, secret).add_grant(orig.clone());
    let token = at.to_jwt().unwrap();

    let mut r = new_request("/");
    let mut w = HeaderMap::new();
    set_authorization_token(&mut r, &token);
    let grants = match m.serve_http(&mut w, &mut r) {
        Ok(()) => handler(&r),
        Err(_) => None,
    };

    assert!(grants.is_some());
    assert_eq!(Some(&orig), grants.unwrap().video.as_ref());

    // no authorization == no claims
    let mut w = HeaderMap::new();
    let mut r = new_request("/");
    let res = m.serve_http(&mut w, &mut r);
    let mut code = StatusCode::OK;
    let mut grants = None;
    match res {
        Ok(()) => grants = handler(&r),
        Err(e) => code = e.status,
    }
    assert!(grants.is_none());
    assert_eq!(StatusCode::OK, code);

    // incorrect authorization: error
    let mut w = HeaderMap::new();
    let mut r = new_request("/");
    set_authorization_token(&mut r, "invalid token");
    let res = m.serve_http(&mut w, &mut r);
    let mut code = StatusCode::OK;
    let mut grants = None;
    match res {
        Ok(()) => grants = handler(&r),
        Err(e) => code = e.status,
    }
    assert!(grants.is_none());
    assert_eq!(StatusCode::UNAUTHORIZED, code);
}

// New for musivum: what the middleware answers, with the statuses, texts and headers of a
// run of the original on the same requests.
#[test]
fn test_auth_middleware_answers() {
    let secret = "somesecretencodedinbase62extendto32bytes";
    let provider = Arc::new(crate::protocol::auth::SimpleKeyProvider::new("key", secret));
    let m = ApiKeyAuthMiddleware::new(provider);

    let tok = AccessToken::new("key", secret)
        .set_identity("alice")
        .set_video_grant(VideoGrant {
            room_join: true,
            room: "r".into(),
            ..Default::default()
        })
        .to_jwt()
        .unwrap();
    let other = AccessToken::new("other", secret)
        .set_identity("alice")
        .to_jwt()
        .unwrap();
    let wrong = AccessToken::new("key", "anothersecretencodedinbase62extendto32bytes")
        .set_identity("alice")
        .to_jwt()
        .unwrap();

    let cases = [
        // name, target, Authorization header, status, body, Access-Control-Allow-Origin
        (
            "basic",
            "/rtc/v1".to_string(),
            "Basic abc".to_string(),
            401,
            "invalid authorization header. Must start with Bearer ",
            "",
        ),
        (
            "garbage",
            "/rtc/v1".to_string(),
            "Bearer garbage".to_string(),
            401,
            "invalid authorization token",
            "",
        ),
        (
            "unknown key",
            format!("/rtc/v1?access_token={other}"),
            String::new(),
            401,
            "invalid API key",
            "",
        ),
        (
            "wrong secret",
            "/rtc/validate".to_string(),
            format!("Bearer {wrong}"),
            401,
            "invalid authorization token: token signature is invalid: signature is invalid",
            "*",
        ),
        (
            "good query",
            format!("/rtc/v1/validate?access_token={tok}"),
            String::new(),
            200,
            "",
            "*",
        ),
        (
            "header wins",
            format!("/rtc/v1?access_token={tok}"),
            "Bearer garbage".to_string(),
            401,
            "invalid authorization token",
            "",
        ),
    ];
    for (name, target, header, status, body, cors) in cases {
        let mut r = new_request(&target);
        if !header.is_empty() {
            r.headers.insert("Authorization", header.parse().unwrap());
        }
        let mut w = HeaderMap::new();
        let (got_status, got_body) = match m.serve_http(&mut w, &mut r) {
            Ok(()) => (200, String::new()),
            Err(e) => (e.status.as_u16(), e.body),
        };
        assert_eq!(status, got_status, "{name}");
        assert_eq!(body, got_body, "{name}");
        assert_eq!(
            cors,
            w.get("Access-Control-Allow-Origin")
                .map(|v| v.to_str().unwrap())
                .unwrap_or(""),
            "{name}"
        );
        assert_eq!(status == 200, get_grants(&r.extensions).is_some(), "{name}");
        if status == 200 {
            assert_eq!("key", get_api_key(&r.extensions), "{name}");
            assert_eq!(
                "alice",
                get_grants(&r.extensions).unwrap().identity,
                "{name}"
            );
            assert!(!get_token_expires_at(&r.extensions).is_zero(), "{name}");
        }
    }
}
