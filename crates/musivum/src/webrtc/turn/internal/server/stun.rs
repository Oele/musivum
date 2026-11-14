// Ported from pion/turn, internal/server/stun.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The address is always one `AddrIPPort` can read; an
// IPv4 address in an IPv6 one is written as IPv4, as Go's `XORMappedAddress` does.

use std::sync::Arc;

use super::{Error, Request, build_and_send, build_msg};
use crate::webrtc::stun::message::{BINDING_SUCCESS, Message};
use crate::webrtc::stun::xoraddr::XorMappedAddress;
use crate::webrtc::turn::internal::ipnet;
use rtc::stun::fingerprint::FINGERPRINT;

pub(crate) async fn handle_binding_request(
    req: &Request<'_>,
    stun_msg: &Message,
) -> Result<(), Error> {
    debugf!(req.log, "Received BindingRequest from {}", req.src_addr);

    let (ip, port) = ipnet::addr_ip_port(req.src_addr);

    let attrs = build_msg(
        stun_msg.transaction_id,
        BINDING_SUCCESS,
        vec![
            Arc::new(XorMappedAddress {
                ip: ip.to_canonical(),
                port,
            }),
            Arc::new(FINGERPRINT),
        ],
    );

    build_and_send(req.conn.as_ref(), req.src_addr, attrs).await
}
