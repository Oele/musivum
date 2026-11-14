// Ported from pion/turn, internal/server/turn.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust, on tokio: the handlers are async (they send).
// - The attributes are the WebRTC library's (`rtc-stun`, `rtc-turn`); an IPv4 address in an
//   IPv6 one is written as IPv4 where Go's attributes do so by themselves.
// - `stunMsg.ForEach(t, f)` is a loop over the attributes of type `t`, each handed to `f` as a
//   copy of the message whose attributes start with it, as pion/stun does.
// - `handleConnectRequest` and `handleConnectionBindRequest` (RFC 6062) are not ported, nor
//   `ipMatchesFamily`'s other callers there.
// - A reservation token is eight random letters as `randutil.GenerateCryptoRandomString`
//   makes them, from `rand`'s generator for keys.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use rand::RngExt;

use super::{
    Attr, Error, Request, authenticate_request, build_and_send, build_and_send_err, build_msg,
    error_code, message_integrity,
};
use crate::webrtc::stun::Error as StunError;
use crate::webrtc::stun::message::{
    CLASS_ERROR_RESPONSE, CLASS_SUCCESS_RESPONSE, Getter, METHOD_ALLOCATE, METHOD_CHANNEL_BIND,
    METHOD_CREATE_PERMISSION, METHOD_REFRESH, Message, MessageType,
};
use crate::webrtc::stun::xoraddr::XorMappedAddress;
use crate::webrtc::turn::internal::allocation::{self, FiveTuple, Permission, Protocol};
use crate::webrtc::turn::internal::ipnet;
use crate::webrtc::turn::internal::proto::{
    self, ChannelData, ChannelNumber, Data, EvenPort, Lifetime, PeerAddress, REQUESTED_FAMILY_IPV4,
    REQUESTED_FAMILY_IPV6, RelayedAddress, RequestedAddressFamily, RequestedTransport,
    ReservationToken,
};
use crate::webrtc::turn::net::{Addr, is_ipv4};
use rtc::stun::attributes::{
    ATTR_DONT_FRAGMENT, ATTR_REQUESTED_ADDRESS_FAMILY, ATTR_RESERVATION_TOKEN,
    ATTR_XOR_PEER_ADDRESS,
};
use rtc::stun::checks::{is_attr_size_invalid, is_attr_size_overflow};
use rtc::stun::error_code::{
    CODE_ADDR_FAMILY_NOT_SUPPORTED, CODE_ALLOC_MISMATCH, CODE_ALLOC_QUOTA_REACHED,
    CODE_BAD_REQUEST, CODE_FORBIDDEN, CODE_INSUFFICIENT_CAPACITY, CODE_PEER_ADDR_FAMILY_MISMATCH,
    CODE_UNAUTHORIZED, CODE_UNKNOWN_ATTRIBUTE, CODE_UNSUPPORTED_TRANS_PROTO,
};
use rtc::stun::textattrs::TextAttribute;
use rtc::stun::uattrs::UnknownAttributes;

const RUNES_ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

fn five_tuple_of(req: &Request<'_>) -> FiveTuple {
    FiveTuple {
        src_addr: Some(req.src_addr),
        dst_addr: Some(req.conn.local_addr()),
        protocol: Protocol::Udp,
    }
}

fn no_allocation_found(req: &Request<'_>) -> Error {
    Error::NoAllocationFoundFor(format!("{}:{}", req.src_addr, req.conn.local_addr()))
}

// See: https://tools.ietf.org/html/rfc5766#section-6.2
// .
pub(crate) async fn handle_allocate_request(
    req: &Request<'_>,
    stun_msg: &Message,
) -> Result<(), Error> {
    debugf!(req.log, "Received AllocateRequest from {}", req.src_addr);

    // 1. The server MUST require that the request be authenticated.  This
    //    authentication MUST be done using the long-term credential
    //    mechanism of [https://tools.ietf.org/html/rfc5389#section-10.2.2]
    //    unless the client and server agree to use another mechanism through
    //    some procedure outside the scope of this document.
    let Some((message_integrity_key, user_id)) =
        authenticate_request(req, stun_msg, METHOD_ALLOCATE).await?
    else {
        return Ok(());
    };
    let message_integrity = message_integrity(&message_integrity_key);

    let five_tuple = five_tuple_of(req);
    let mut requested_port: i64 = 0;
    let mut reservation_token = String::new();

    let bad_request_msg = build_msg(
        stun_msg.transaction_id,
        MessageType::new(METHOD_ALLOCATE, CLASS_ERROR_RESPONSE),
        vec![error_code(CODE_BAD_REQUEST)],
    );
    let insufficient_capacity_msg = build_msg(
        stun_msg.transaction_id,
        MessageType::new(METHOD_ALLOCATE, CLASS_ERROR_RESPONSE),
        vec![error_code(CODE_INSUFFICIENT_CAPACITY)],
    );
    let conn = req.conn.as_ref();

    // 2. The server checks if the 5-tuple is currently in use by an
    //    existing allocation.  If yes, the server rejects the request with
    //    a 437 (Allocation Mismatch) error.
    if let Some(alloc) = req.allocation_manager.get_allocation(&five_tuple) {
        let (id, mut attrs) = alloc.get_response_cache();
        if id != stun_msg.transaction_id.0 {
            let msg = build_msg(
                stun_msg.transaction_id,
                MessageType::new(METHOD_ALLOCATE, CLASS_ERROR_RESPONSE),
                vec![error_code(CODE_ALLOC_MISMATCH)],
            );

            return Err(build_and_send_err(
                conn,
                req.src_addr,
                Error::RelayAlreadyAllocatedForFiveTuple,
                msg,
            )
            .await);
        }
        // A retry allocation
        attrs.push(message_integrity);
        let msg = build_msg(
            stun_msg.transaction_id,
            MessageType::new(METHOD_ALLOCATE, CLASS_SUCCESS_RESPONSE),
            attrs,
        );

        return build_and_send(conn, req.src_addr, msg).await;
    }

    // 3. The server checks if the request contains a REQUESTED-TRANSPORT
    //    attribute.  If the REQUESTED-TRANSPORT attribute is not included
    //    or is malformed, the server rejects the request with a 400 (Bad
    //    Request) error.  Otherwise, if the attribute is included but
    //    specifies a protocol other that UDP/TCP, the server rejects the
    //    request with a 442 (Unsupported Transport Protocol) error.
    let mut requested_transport = RequestedTransport::default();
    if let Err(err) = requested_transport.get_from(stun_msg) {
        return Err(build_and_send_err(conn, req.src_addr, err.into(), bad_request_msg).await);
    } else if requested_transport.protocol != proto::PROTO_UDP
        && requested_transport.protocol != proto::PROTO_TCP
    {
        let msg = build_msg(
            stun_msg.transaction_id,
            MessageType::new(METHOD_ALLOCATE, CLASS_ERROR_RESPONSE),
            vec![error_code(CODE_UNSUPPORTED_TRANS_PROTO)],
        );

        return Err(build_and_send_err(
            conn,
            req.src_addr,
            Error::UnsupportedTransportProtocol,
            msg,
        )
        .await);
    }

    // 4. The request may contain a DONT-FRAGMENT attribute.  If it does,
    //    but the server does not support sending UDP datagrams with the DF
    //    bit set to 1 (see Section 12), then the server treats the DONT-
    //    FRAGMENT attribute in the Allocate request as an unknown
    //    comprehension-required attribute.
    if stun_msg.contains(ATTR_DONT_FRAGMENT) {
        let msg = build_msg(
            stun_msg.transaction_id,
            MessageType::new(METHOD_ALLOCATE, CLASS_ERROR_RESPONSE),
            vec![
                error_code(CODE_UNKNOWN_ATTRIBUTE),
                Arc::new(UnknownAttributes(vec![ATTR_DONT_FRAGMENT])),
            ],
        );

        return Err(
            build_and_send_err(conn, req.src_addr, Error::NoDontFragmentSupport, msg).await,
        );
    }

    // 5.  The server checks if the request contains a RESERVATION-TOKEN
    //     attribute.  If yes, and the request also contains an EVEN-PORT
    //     attribute, then the server rejects the request with a 400 (Bad
    //     Request) error.  Otherwise, it checks to see if the token is
    //     valid (i.e., the token is in range and has not expired and the
    //     corresponding relayed transport address is still available).  If
    //     the token is not valid for some reason, the server rejects the
    //     request with a 508 (Insufficient Capacity) error.
    let mut reservation_token_attr = ReservationToken::default();
    if reservation_token_attr.get_from(stun_msg).is_ok() {
        let mut even_port = EvenPort::default();
        if even_port.get_from(stun_msg).is_ok() {
            return Err(build_and_send_err(
                conn,
                req.src_addr,
                Error::RequestWithReservationTokenAndEvenPort,
                bad_request_msg,
            )
            .await);
        }

        let (allocation_port, reservation_found) = req
            .allocation_manager
            .get_reservation(&String::from_utf8_lossy(&reservation_token_attr.0));
        if !reservation_found {
            return Err(build_and_send_err(
                conn,
                req.src_addr,
                Error::NoAllocationFound,
                insufficient_capacity_msg,
            )
            .await);
        }
        requested_port = allocation_port + 1;
    }

    // 6. The server checks if the request contains an EVEN-PORT attribute.
    //    If yes, then the server checks that it can satisfy the request
    //    (i.e., can allocate a relayed transport address as described
    //    below).  If the server cannot satisfy the request, then the
    //    server rejects the request with a 508 (Insufficient Capacity)
    //    error.
    let mut even_port = EvenPort::default();
    if even_port.get_from(stun_msg).is_ok() {
        let random_port = match req.allocation_manager.get_random_even_port() {
            Ok(p) => p,
            Err(err) => {
                return Err(build_and_send_err(
                    conn,
                    req.src_addr,
                    err.into(),
                    insufficient_capacity_msg,
                )
                .await);
            }
        };
        requested_port = random_port;
        reservation_token = generate_crypto_random_string(8, RUNES_ALPHA);
    }

    // Parse realm (already checked in authenticateRequest)
    let realm_attr =
        TextAttribute::get_from_as(stun_msg, rtc::stun::attributes::ATTR_REALM).unwrap_or_default();

    // RFC 6156: Parse REQUESTED-ADDRESS-FAMILY attribute if present.
    // If absent, default to the listener family unless strict RFC 6156
    // behavior is requested.
    // "Length:  this 16-bit field contains the length of the attribute in
    // bytes.  The length of this attribute is 4 bytes."
    // "If the server does not support the address family requested by the
    // client, it MUST generate an Allocate error response, and it MUST
    // include an ERROR-CODE attribute with the 440 (Address Family not
    // Supported) response code, which is defined in Section 4.2.1."
    let mut requested_family = RequestedAddressFamily::default();
    if let Err(err) = requested_family.get_from(stun_msg) {
        if err == StunError::ErrAttributeNotFound {
            requested_family = default_allocation_address_family(req);
        } else if is_attr_size_invalid(&err) || is_attr_size_overflow(&err) {
            return Err(build_and_send_err(conn, req.src_addr, err.into(), bad_request_msg).await);
        } else {
            let msg = build_msg(
                stun_msg.transaction_id,
                MessageType::new(METHOD_ALLOCATE, CLASS_ERROR_RESPONSE),
                vec![error_code(CODE_ADDR_FAMILY_NOT_SUPPORTED)],
            );

            return Err(build_and_send_err(
                conn,
                req.src_addr,
                Error::UnsupportedAddressFamily,
                msg,
            )
            .await);
        }
    }

    // RFC 6156: Check if the requested address family is supported.
    // If not, reject with 440 (Address Family not Supported) error.
    if requested_family != REQUESTED_FAMILY_IPV4 && requested_family != REQUESTED_FAMILY_IPV6 {
        let msg = build_msg(
            stun_msg.transaction_id,
            MessageType::new(METHOD_ALLOCATE, CLASS_ERROR_RESPONSE),
            vec![error_code(CODE_ADDR_FAMILY_NOT_SUPPORTED)],
        );

        return Err(
            build_and_send_err(conn, req.src_addr, Error::UnsupportedAddressFamily, msg).await,
        );
    }

    // RFC 6156: REQUESTED-ADDRESS-FAMILY and RESERVATION-TOKEN are mutually exclusive.
    if stun_msg.contains(ATTR_RESERVATION_TOKEN) && stun_msg.contains(ATTR_REQUESTED_ADDRESS_FAMILY)
    {
        return Err(build_and_send_err(
            conn,
            req.src_addr,
            Error::RequestWithReservationTokenAndRequestedFamily,
            bad_request_msg,
        )
        .await);
    }

    // 7. At any point, the server MAY choose to reject the request with a
    //    486 (Allocation Quota Reached) error if it feels the client is
    //    trying to exceed some locally defined allocation quota.  The
    //    server is free to define this allocation quota any way it wishes,
    //    but SHOULD define it based on the username used to authenticate
    //    the request, and not on the client's transport address.
    if let Some(quota_handler) = &req.quota_handler
        && !quota_handler(&user_id, &realm_attr.text, req.src_addr)
    {
        let quota_reached_msg = build_msg(
            stun_msg.transaction_id,
            MessageType::new(METHOD_ALLOCATE, CLASS_ERROR_RESPONSE),
            vec![error_code(CODE_ALLOC_QUOTA_REACHED)],
        );

        return build_and_send(conn, req.src_addr, quota_reached_msg).await;
    }

    // 8. Also at any point, the server MAY choose to reject the request
    //    with a 300 (Try Alternate) error if it wishes to redirect the
    //    client to a different server.  The use of this error code and
    //    attribute follow the specification in [RFC5389].
    let lifetime_duration = super::allocation_life_time(req, stun_msg);
    let alloc = match req.allocation_manager.create_allocation(
        Some(&five_tuple),
        Some(req.conn.clone()),
        requested_transport.protocol,
        requested_port,
        lifetime_duration,
        &user_id,
        &realm_attr.text,
        requested_family,
    ) {
        Ok(alloc) => alloc,
        Err(err) => {
            return Err(build_and_send_err(
                conn,
                req.src_addr,
                err.into(),
                insufficient_capacity_msg,
            )
            .await);
        }
    };

    // Once the allocation is created, the server replies with a success
    // response.
    // The success response contains:
    //   * An XOR-RELAYED-ADDRESS attribute containing the relayed transport
    //     address.
    //   * A LIFETIME attribute containing the current value of the time-to-
    //     expiry timer.
    //   * A RESERVATION-TOKEN attribute (if a second relayed transport
    //     address was reserved).
    //   * An XOR-MAPPED-ADDRESS attribute containing the client's IP address
    //     and port (from the 5-tuple).

    let (src_ip, src_port) = ipnet::addr_ip_port(req.src_addr);

    // The relay address of an allocation without one (neither UDP nor TCP) is what
    // `AddrIPPort` fails on in the original.
    let Some(relay_addr) = alloc.relay_addr else {
        return Err(build_and_send_err(
            conn,
            req.src_addr,
            Error::FailedToCastAddr,
            bad_request_msg,
        )
        .await);
    };
    let (relay_ip, relay_port) = ipnet::addr_ip_port(relay_addr);

    let mut response_attrs: Vec<Attr> = vec![
        Arc::new(RelayedAddress {
            ip: relay_ip.to_canonical(),
            port: relay_port,
        }),
        Arc::new(Lifetime(lifetime_duration.to_std())),
        Arc::new(XorMappedAddress {
            ip: src_ip.to_canonical(),
            port: src_port,
        }),
    ];

    if !reservation_token.is_empty() {
        req.allocation_manager
            .create_reservation(&reservation_token, relay_port as i64);
        response_attrs.push(Arc::new(ReservationToken(reservation_token.into_bytes())));
    }

    let mut attrs = response_attrs.clone();
    attrs.push(message_integrity);
    let msg = build_msg(
        stun_msg.transaction_id,
        MessageType::new(METHOD_ALLOCATE, CLASS_SUCCESS_RESPONSE),
        attrs,
    );
    alloc.set_response_cache(stun_msg.transaction_id.0, response_attrs);

    build_and_send(conn, req.src_addr, msg).await
}

pub(crate) async fn handle_refresh_request(
    req: &Request<'_>,
    stun_msg: &Message,
) -> Result<(), Error> {
    debugf!(req.log, "Received RefreshRequest from {}", req.src_addr);

    let Some((message_integrity_key, user_id)) =
        authenticate_request(req, stun_msg, METHOD_REFRESH).await?
    else {
        return Ok(());
    };
    let conn = req.conn.as_ref();

    let lifetime_duration = super::allocation_life_time(req, stun_msg);
    let five_tuple = five_tuple_of(req);

    let Some(a) = req
        .allocation_manager
        .get_allocation_for_user_id(&five_tuple, &user_id)
    else {
        return Err(no_allocation_found(req));
    };

    // RFC 6156: If REQUESTED-ADDRESS-FAMILY is present in Refresh request,
    // it must match the allocation's address family.
    // "The client MUST NOT include any REQUESTED-ADDRESS-FAMILY attribute in
    // its Refresh Request."
    // "If a server receives a Refresh Request with a REQUESTED-ADDRESS-
    // FAMILY attribute, and the attribute's value doesn't match the address
    // family of the allocation, the server MUST reply with a 443 (Peer
    // Address Family Mismatch) Refresh error response."
    let mut requested_family = RequestedAddressFamily::default();
    let bad_request_msg = build_msg(
        stun_msg.transaction_id,
        MessageType::new(METHOD_REFRESH, CLASS_ERROR_RESPONSE),
        vec![error_code(CODE_BAD_REQUEST)],
    );
    match requested_family.get_from(stun_msg) {
        Err(err) => {
            if err == StunError::ErrAttributeNotFound {
            } else if is_attr_size_invalid(&err) || is_attr_size_overflow(&err) {
                return Err(
                    build_and_send_err(conn, req.src_addr, err.into(), bad_request_msg).await,
                );
            } else {
                let msg = build_msg(
                    stun_msg.transaction_id,
                    MessageType::new(METHOD_REFRESH, CLASS_ERROR_RESPONSE),
                    vec![error_code(CODE_PEER_ADDR_FAMILY_MISMATCH)],
                );

                return Err(build_and_send_err(
                    conn,
                    req.src_addr,
                    Error::PeerAddressFamilyMismatch,
                    msg,
                )
                .await);
            }
        }
        Ok(()) if requested_family != a.address_family() => {
            let msg = build_msg(
                stun_msg.transaction_id,
                MessageType::new(METHOD_REFRESH, CLASS_ERROR_RESPONSE),
                vec![error_code(CODE_PEER_ADDR_FAMILY_MISMATCH)],
            );

            return Err(build_and_send_err(
                conn,
                req.src_addr,
                Error::PeerAddressFamilyMismatch,
                msg,
            )
            .await);
        }
        Ok(()) => {}
    }

    if lifetime_duration != crate::go::time::Duration::ZERO {
        a.refresh(lifetime_duration);
    } else {
        req.allocation_manager.delete_allocation(&five_tuple);
    }

    build_and_send(
        conn,
        req.src_addr,
        build_msg(
            stun_msg.transaction_id,
            MessageType::new(METHOD_REFRESH, CLASS_SUCCESS_RESPONSE),
            vec![
                Arc::new(Lifetime(lifetime_duration.to_std())),
                message_integrity(&message_integrity_key),
            ],
        ),
    )
    .await
}

pub(crate) async fn handle_create_permission_request(
    req: &Request<'_>,
    stun_msg: &Message,
) -> Result<(), Error> {
    debugf!(req.log, "Received CreatePermission from {}", req.src_addr);

    let Some((message_integrity_key, user_id)) =
        authenticate_request(req, stun_msg, METHOD_CREATE_PERMISSION).await?
    else {
        return Ok(());
    };

    let Some(alloc) = req
        .allocation_manager
        .get_allocation_for_user_id(&five_tuple_of(req), &user_id)
    else {
        return Err(no_allocation_found(req));
    };

    let mut add_count = 0;
    let mut error_code_value = CODE_BAD_REQUEST;

    // stunMsg.ForEach(stun.AttrXORPeerAddress, func(m *stun.Message) error { ... })
    let mut for_each = || -> Result<(), Error> {
        for (i, a) in stun_msg.attributes.0.iter().enumerate() {
            if a.typ != ATTR_XOR_PEER_ADDRESS {
                continue;
            }
            let mut m = stun_msg.clone();
            m.attributes.0 = stun_msg.attributes.0[i..].to_vec();

            let mut peer_address = PeerAddress::default();
            if let Err(err) = peer_address.get_from(&m) {
                error_code_value = CODE_BAD_REQUEST;

                return Err(err.into());
            }

            // RFC 6156: Peer address must match allocation's address family.
            if !ip_matches_family(peer_address.ip, &alloc.address_family()) {
                infof!(
                    req.log,
                    "peer address family mismatch for client {} to peer {}",
                    req.src_addr,
                    peer_address.ip
                );
                error_code_value = CODE_PEER_ADDR_FAMILY_MISMATCH;

                return Err(Error::PeerAddressFamilyMismatch);
            }

            if let Err(err) = req
                .allocation_manager
                .grant_permission(req.src_addr, peer_address.ip)
            {
                infof!(
                    req.log,
                    "permission denied for client {} to peer {}",
                    req.src_addr,
                    peer_address.ip
                );
                error_code_value = CODE_FORBIDDEN;

                return Err(err.into());
            }

            debugf!(
                req.log,
                "Adding permission for {}",
                SocketAddr::new(peer_address.ip, peer_address.port)
            );

            alloc.add_permission(Permission::new(
                Addr::Udp(SocketAddr::new(peer_address.ip, peer_address.port)),
                req.log.clone(),
                req.permission_timeout,
            ));
            add_count += 1;
        }

        Ok(())
    };
    if for_each().is_err() {
        add_count = 0;
    }

    let mut resp_class = CLASS_SUCCESS_RESPONSE;
    if add_count == 0 {
        resp_class = CLASS_ERROR_RESPONSE;
    }
    let mut attrs = vec![message_integrity(&message_integrity_key)];
    if resp_class == CLASS_ERROR_RESPONSE {
        attrs = vec![
            error_code(error_code_value),
            message_integrity(&message_integrity_key),
        ];
    }

    build_and_send(
        req.conn.as_ref(),
        req.src_addr,
        build_msg(
            stun_msg.transaction_id,
            MessageType::new(METHOD_CREATE_PERMISSION, resp_class),
            attrs,
        ),
    )
    .await
}

pub(crate) async fn handle_send_indication(
    req: &Request<'_>,
    stun_msg: &Message,
) -> Result<(), Error> {
    debugf!(req.log, "Received SendIndication from {}", req.src_addr);
    let Some(alloc) = req.allocation_manager.get_allocation(&five_tuple_of(req)) else {
        return Err(no_allocation_found(req));
    };

    let mut data_attr = Data::default();
    data_attr.get_from(stun_msg)?;

    let mut peer_address = PeerAddress::default();
    peer_address.get_from(stun_msg)?;

    let msg_dst = Addr::Udp(SocketAddr::new(peer_address.ip, peer_address.port));
    if alloc.get_permission(msg_dst).is_none() {
        return Err(Error::NoPermission(msg_dst.to_string()));
    }

    match alloc.write_to(&data_attr.0, msg_dst).await {
        Err(err) => Err(Error::FailedWriteSocket(err.to_string())),
        Ok(l) if l != data_attr.0.len() => Err(Error::ShortWrite(l, data_attr.0.len())),
        Ok(_) => Ok(()),
    }
}

pub(crate) async fn handle_channel_bind_request(
    req: &Request<'_>,
    stun_msg: &Message,
) -> Result<(), Error> {
    debugf!(req.log, "Received ChannelBindRequest from {}", req.src_addr);

    let bad_request_msg = build_msg(
        stun_msg.transaction_id,
        MessageType::new(METHOD_CHANNEL_BIND, CLASS_ERROR_RESPONSE),
        vec![error_code(CODE_BAD_REQUEST)],
    );
    let conn = req.conn.as_ref();

    let Some((message_integrity_key, user_id)) =
        authenticate_request(req, stun_msg, METHOD_CHANNEL_BIND).await?
    else {
        return Ok(());
    };

    let Some(alloc) = req
        .allocation_manager
        .get_allocation_for_user_id(&five_tuple_of(req), &user_id)
    else {
        return Err(no_allocation_found(req));
    };

    let mut channel = ChannelNumber::default();
    if let Err(err) = channel.get_from(stun_msg) {
        return Err(build_and_send_err(conn, req.src_addr, err.into(), bad_request_msg).await);
    }

    let mut peer_addr = PeerAddress::default();
    if let Err(err) = peer_addr.get_from(stun_msg) {
        return Err(build_and_send_err(conn, req.src_addr, err.into(), bad_request_msg).await);
    }

    // RFC 6156: Peer address must match allocation's address family.
    if !ip_matches_family(peer_addr.ip, &alloc.address_family()) {
        infof!(
            req.log,
            "peer address family mismatch for client {} to peer {}",
            req.src_addr,
            peer_addr.ip
        );

        let peer_addr_family_mismatch_msg = build_msg(
            stun_msg.transaction_id,
            MessageType::new(METHOD_CHANNEL_BIND, CLASS_ERROR_RESPONSE),
            vec![error_code(CODE_PEER_ADDR_FAMILY_MISMATCH)],
        );

        return Err(build_and_send_err(
            conn,
            req.src_addr,
            Error::PeerAddressFamilyMismatch,
            peer_addr_family_mismatch_msg,
        )
        .await);
    }

    if let Err(err) = req
        .allocation_manager
        .grant_permission(req.src_addr, peer_addr.ip)
    {
        infof!(
            req.log,
            "permission denied for client {} to peer {}",
            req.src_addr,
            peer_addr.ip
        );

        let unauthorized_request_msg = build_msg(
            stun_msg.transaction_id,
            MessageType::new(METHOD_CHANNEL_BIND, CLASS_ERROR_RESPONSE),
            vec![error_code(CODE_UNAUTHORIZED)],
        );

        return Err(
            build_and_send_err(conn, req.src_addr, err.into(), unauthorized_request_msg).await,
        );
    }

    let peer_net_addr = Addr::Udp(SocketAddr::new(peer_addr.ip, peer_addr.port));
    debugf!(req.log, "Binding channel {} to {}", channel.0, peer_addr);
    if let Err(err) = alloc.add_channel_bind(
        allocation::ChannelBind::new(channel, peer_net_addr, req.log.clone()),
        req.channel_bind_timeout,
        req.permission_timeout,
    ) {
        if matches!(err, allocation::Error::SamePeerDifferentChannel) {
            if let Some(existing) = alloc.get_channel_by_addr(peer_net_addr) {
                warnf!(
                    req.log,
                    "ChannelBind rejected: peer {} already bound to channel {}, attempted {}",
                    peer_net_addr,
                    existing.number.0,
                    channel.0
                );
            } else {
                warnf!(
                    req.log,
                    "ChannelBind rejected: peer {} attempted to rebind to channel {}",
                    peer_net_addr,
                    channel.0
                );
            }
        } else if matches!(err, allocation::Error::SameChannelDifferentPeer) {
            if let Some(existing) = alloc.get_channel_by_number(channel) {
                warnf!(
                    req.log,
                    "ChannelBind rejected: channel {} already bound to peer {}, attempted peer {}",
                    channel.0,
                    existing.peer,
                    peer_net_addr
                );
            } else {
                warnf!(
                    req.log,
                    "ChannelBind rejected: channel {} attempted to bind to peer {}",
                    channel.0,
                    peer_net_addr
                );
            }
        }

        return Err(build_and_send_err(conn, req.src_addr, err.into(), bad_request_msg).await);
    }

    build_and_send(
        conn,
        req.src_addr,
        build_msg(
            stun_msg.transaction_id,
            MessageType::new(METHOD_CHANNEL_BIND, CLASS_SUCCESS_RESPONSE),
            vec![message_integrity(&message_integrity_key)],
        ),
    )
    .await
}

pub(crate) async fn handle_channel_data(
    req: &Request<'_>,
    channel_data: &ChannelData,
) -> Result<(), Error> {
    debugf!(req.log, "Received ChannelData from {}", req.src_addr);

    let Some(alloc) = req.allocation_manager.get_allocation(&five_tuple_of(req)) else {
        return Err(no_allocation_found(req));
    };

    let Some(channel) = alloc.get_channel_by_number(channel_data.number) else {
        return Err(Error::NoSuchChannelBind(channel_data.number.0));
    };

    match alloc.write_to(&channel_data.data, channel.peer).await {
        Err(err) => Err(Error::FailedWriteSocket(err.to_string())),
        Ok(l) if l != channel_data.data.len() => Err(Error::ShortWrite(l, channel_data.data.len())),
        Ok(_) => Ok(()),
    }
}

// Not here: handleConnectRequest and handleConnectionBindRequest (RFC 6062), see the header.

// ipMatchesFamily checks if an IP address matches the given address family.
pub(crate) fn ip_matches_family(ip: IpAddr, family: &RequestedAddressFamily) -> bool {
    if *family == REQUESTED_FAMILY_IPV4 {
        return is_ipv4(ip);
    }
    if *family == REQUESTED_FAMILY_IPV6 {
        return !is_ipv4(ip);
    }

    false
}

pub(crate) fn default_allocation_address_family(req: &Request<'_>) -> RequestedAddressFamily {
    if req.strict_address_family {
        return REQUESTED_FAMILY_IPV4;
    }

    let (ip, _) = ipnet::addr_ip_port(req.conn.local_addr());

    // some dual-stack wildcard sockets can report [::] as LocalAddr even for IPv4 or
    // ipv6-mapped IPv4 addresses.
    if ip.to_canonical().is_unspecified() {
        let (family, ok) = address_family_from_addr(req.src_addr);
        if ok {
            return family;
        }
    }

    let (family, ok) = address_family_from_ip(ip);
    if ok {
        return family;
    }

    REQUESTED_FAMILY_IPV4
}

fn address_family_from_addr(addr: Addr) -> (RequestedAddressFamily, bool) {
    let (ip, _) = ipnet::addr_ip_port(addr);

    address_family_from_ip(ip)
}

fn address_family_from_ip(ip: IpAddr) -> (RequestedAddressFamily, bool) {
    if is_ipv4(ip) {
        return (REQUESTED_FAMILY_IPV4, true);
    }

    (REQUESTED_FAMILY_IPV6, true)
}

// randutil.GenerateCryptoRandomString(n, runes)
fn generate_crypto_random_string(n: usize, runes: &[u8]) -> String {
    let mut rng = rand::rng();
    (0..n)
        .map(|_| runes[rng.random_range(0..runes.len())] as char)
        .collect()
}

#[cfg(test)]
#[path = "turn_test.rs"]
mod turn_test;
