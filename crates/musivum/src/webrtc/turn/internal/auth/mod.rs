// Ported from pion/turn, internal/auth/auth.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The handler is an `Arc` of a function; the TLS
// connection state is `net::ConnectionState`.

// Package auth provides internal authentication / authorization
// types and utilities for the TURN server.

use std::sync::Arc;

use crate::webrtc::stun::message::Method;
use crate::webrtc::turn::net::{Addr, ConnectionState};

// RequestAttributes represents attributes of a TURN request which
// may be useful for authorizing the underlying request.
#[derive(Clone, Debug)]
pub struct RequestAttributes {
    pub username: String,
    pub realm: String,
    pub src_addr: Addr,
    pub tls: Option<ConnectionState>,
    pub method: Method,
    // extend as needed
}

// AuthHandler is a callback used to handle incoming auth requests,
// allowing users to customize Pion TURN with custom behavior.
pub type AuthHandler = Arc<dyn Fn(&RequestAttributes) -> (String, Vec<u8>, bool) + Send + Sync>;
