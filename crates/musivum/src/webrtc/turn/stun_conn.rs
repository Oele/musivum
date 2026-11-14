// Ported from pion/turn, stun_conn.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust.

use super::internal::proto;
use super::net::Conn;

pub type StunConn = proto::StunConn;

// NewSTUNConn creates a STUNConn.
pub fn new_stun_conn(next_conn: Box<dyn Conn>) -> StunConn {
    proto::StunConn::new(next_conn)
}
