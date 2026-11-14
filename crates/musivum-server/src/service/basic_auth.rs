// Ported from LiveKit, pkg/service/basic_auth.go at v1.13.11.
// Copyright LiveKit, Inc. Licensed under the Apache License, Version 2.0 (the file has no
// header of its own; the repository's licence).
// Changes: translated from Go to Rust, as a middleware of axum where the original is one of
// negroni. Go's `r.BasicAuth()` is `basic_auth` below, written new after RFC 7617 and what
// Go's net/http takes: the first `Authorization` header, its scheme `Basic ` in any case,
// the credentials in standard base64 with padding (carriage returns and newlines passed
// over, as Go's decoder does), cut at the first colon.

use axum::extract::Request;
use axum::http::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// `GenBasicAuthMiddleware(username, password)`: lets a request through to `next` only with
/// these credentials, and answers 401 with a challenge otherwise. Use it with
/// `axum::middleware::from_fn`.
pub fn gen_basic_auth_middleware(
    username: String,
    password: String,
) -> impl Fn(Request, Next) -> std::pin::Pin<Box<dyn Future<Output = Response> + Send>>
+ Clone
+ Send
+ Sync
+ 'static {
    move |r: Request, next: Next| {
        let given = basic_auth(r.headers());
        let unauthorized = || {
            (
                StatusCode::UNAUTHORIZED,
                [(WWW_AUTHENTICATE, "Basic realm=\"Protected Area\"")],
            )
                .into_response()
        };
        let Some((given_username, given_password)) = given else {
            let res = unauthorized();
            return Box::pin(async move { res });
        };

        if given_username != username.as_bytes() {
            let res = unauthorized();
            return Box::pin(async move { res });
        }

        if given_password != password.as_bytes() {
            let res = unauthorized();
            return Box::pin(async move { res });
        }

        Box::pin(next.run(r))
    }
}

/// Go's `r.BasicAuth()`: the user name and password of the request's basic authentication,
/// as bytes (Go's strings), if it has one that can be read.
pub fn basic_auth(headers: &HeaderMap) -> Option<(Vec<u8>, Vec<u8>)> {
    const PREFIX: &[u8] = b"Basic ";
    let auth = headers.get(AUTHORIZATION)?.as_bytes();
    if auth.len() < PREFIX.len() || !auth[..PREFIX.len()].eq_ignore_ascii_case(PREFIX) {
        return None;
    }
    let c = decode_std_base64(&auth[PREFIX.len()..])?;
    let colon = c.iter().position(|b| *b == b':')?;
    Some((c[..colon].to_vec(), c[colon + 1..].to_vec()))
}

// Go's base64.StdEncoding.DecodeString: the standard alphabet, padded to whole groups of
// four; carriage returns and newlines are passed over, and the bits left over after the last
// byte need not be zero.
fn decode_std_base64(s: &[u8]) -> Option<Vec<u8>> {
    let s: Vec<u8> = s
        .iter()
        .copied()
        .filter(|b| *b != b'\r' && *b != b'\n')
        .collect();
    if !s.len().is_multiple_of(4) {
        return None;
    }
    let padding = s.iter().rev().take_while(|b| **b == b'=').count();
    if padding > 2 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    let mut bits: u32 = 0;
    let mut nbits = 0;
    for &c in &s[..s.len() - padding] {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        };
        bits = (bits << 6) | u32::from(v);
        nbits += 6;
        if nbits >= 8 {
            nbits -= 8;
            out.push((bits >> nbits) as u8);
            bits &= (1 << nbits) - 1;
        }
    }
    Some(out)
}

#[cfg(test)]
#[path = "basic_auth_test.rs"]
mod basic_auth_test;
