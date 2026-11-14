// Ported from pion/turn, internal/server/server.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust, on tokio: handling a request is async (it sends).
// - `Request` borrows the datagram; its handlers and the nonce manager are `Arc`s.
// - `getMessageHandler` gives a `MessageHandler`, which `handle` calls. Connect and
//   ConnectionBind (RFC 6062) are not ported: they are unexpected methods here.
// - pion/stun's `AttrType.Known()` is `known`, with the attributes pion/stun knows.

use std::sync::Arc;

use super::{
    Error, NonceManager, build_and_send, build_msg, handle_allocate_request,
    handle_binding_request, handle_channel_bind_request, handle_channel_data,
    handle_create_permission_request, handle_refresh_request, handle_send_indication,
};
use crate::go::time::Duration;
use crate::protocol::logger::Logger;
use crate::webrtc::stun::message::{
    CLASS_ERROR_RESPONSE, CLASS_INDICATION, CLASS_REQUEST, METHOD_ALLOCATE, METHOD_BINDING,
    METHOD_CHANNEL_BIND, METHOD_CREATE_PERMISSION, METHOD_REFRESH, METHOD_SEND, Message,
    MessageClass, MessageType, Method,
};
use crate::webrtc::turn::internal::allocation::Manager;
use crate::webrtc::turn::internal::auth::AuthHandler;
use crate::webrtc::turn::internal::proto::{self, ChannelData};
use crate::webrtc::turn::net::{Addr, ConnectionState, PacketConn};
use rtc::stun::attributes::*;
use rtc::stun::error_code::CODE_UNKNOWN_ATTRIBUTE;
use rtc::stun::uattrs::UnknownAttributes;

/// `func(username string, realm string, srcAddr net.Addr) (ok bool)`.
pub type QuotaHandlerFn = Arc<dyn Fn(&str, &str, Addr) -> bool + Send + Sync>;

// Request contains all the state needed to process a single incoming datagram.
#[derive(Clone)]
pub struct Request<'a> {
    // Current Request State
    pub conn: Arc<dyn PacketConn>,
    pub src_addr: Addr,
    pub buff: &'a [u8],
    pub tls: Option<ConnectionState>,

    // Server State
    pub allocation_manager: Arc<Manager>,
    pub nonce_hash: Arc<dyn NonceManager>,

    // User Configuration
    pub auth_handler: Option<AuthHandler>,

    // Quota Handler
    pub quota_handler: Option<QuotaHandlerFn>,

    pub log: Logger,
    pub realm: String,

    pub channel_bind_timeout: Duration,
    pub permission_timeout: Duration,
    pub allocation_lifetime: Duration,
    pub strict_address_family: bool,
}

// HandleRequest processes the give Request.
pub async fn handle_request(r: &Request<'_>) -> Result<(), Error> {
    debugf!(
        r.log,
        "Received {} bytes of udp from {} on {}",
        r.buff.len(),
        r.src_addr,
        r.conn.local_addr()
    );

    if proto::is_channel_data(r.buff) {
        return handle_data_packet(r).await;
    }

    handle_turn_packet(r).await
}

async fn handle_data_packet(req: &Request<'_>) -> Result<(), Error> {
    debugf!(req.log, "Received DataPacket from {}", req.src_addr);
    let mut c = ChannelData {
        raw: req.buff.to_vec(),
        ..Default::default()
    };
    if let Err(err) = c.decode() {
        return Err(Error::FailedToCreateChannelData(err.to_string()));
    }

    handle_channel_data(req, &c)
        .await
        .map_err(|err| Error::UnableToHandleChannelData(req.src_addr.to_string(), err.to_string()))
}

async fn handle_turn_packet(req: &Request<'_>) -> Result<(), Error> {
    debugf!(req.log, "Handling TURN packet");
    let mut stun_msg = Message::new();
    stun_msg.raw = req.buff.to_vec();
    if let Err(err) = stun_msg.decode() {
        return Err(Error::FailedToCreateStunPacket(err.to_string()));
    }

    let handler = match get_message_handler(stun_msg.typ.class, stun_msg.typ.method) {
        Ok(h) => h,
        Err(err) => {
            return Err(Error::UnhandledStunPacket(
                format!(
                    "{}-{} from {}",
                    stun_msg.typ.method, stun_msg.typ.class, req.src_addr
                ),
                err.to_string(),
            ));
        }
    };

    let mut unknown_attributes = Vec::new();
    for attr in &stun_msg.attributes.0 {
        if attr.typ.required() && !known(attr.typ) {
            unknown_attributes.push(attr.typ);
        }
    }

    if !unknown_attributes.is_empty() {
        errorf!(
            req.log,
            "{}-{} with unknown comprehension-required attributes from {}: {:?}",
            stun_msg.typ.method,
            stun_msg.typ.class,
            req.src_addr,
            unknown_attributes
                .iter()
                .map(|a| a.to_string())
                .collect::<Vec<_>>()
        );

        return build_and_send(
            req.conn.as_ref(),
            req.src_addr,
            build_msg(
                stun_msg.transaction_id,
                MessageType::new(stun_msg.typ.method, CLASS_ERROR_RESPONSE),
                vec![
                    Arc::new(CODE_UNKNOWN_ATTRIBUTE),
                    Arc::new(UnknownAttributes(unknown_attributes)),
                ],
            ),
        )
        .await;
    }

    if let Err(err) = handler.handle(req, &stun_msg).await {
        return Err(Error::FailedToHandle(
            format!(
                "{}-{} from {}",
                stun_msg.typ.method, stun_msg.typ.class, req.src_addr
            ),
            err.to_string(),
        ));
    }

    Ok(())
}

/// What `getMessageHandler` gives: the handler of a class and method.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MessageHandler {
    SendIndication,
    AllocateRequest,
    RefreshRequest,
    CreatePermissionRequest,
    ChannelBindRequest,
    BindingRequest,
}

impl MessageHandler {
    async fn handle(self, req: &Request<'_>, stun_msg: &Message) -> Result<(), Error> {
        match self {
            MessageHandler::SendIndication => handle_send_indication(req, stun_msg).await,
            MessageHandler::AllocateRequest => handle_allocate_request(req, stun_msg).await,
            MessageHandler::RefreshRequest => handle_refresh_request(req, stun_msg).await,
            MessageHandler::CreatePermissionRequest => {
                handle_create_permission_request(req, stun_msg).await
            }
            MessageHandler::ChannelBindRequest => handle_channel_bind_request(req, stun_msg).await,
            MessageHandler::BindingRequest => handle_binding_request(req, stun_msg).await,
        }
    }
}

pub(crate) fn get_message_handler(
    class: MessageClass,
    method: Method,
) -> Result<MessageHandler, Error> {
    match class {
        CLASS_INDICATION => match method {
            METHOD_SEND => Ok(MessageHandler::SendIndication),
            _ => Err(Error::UnexpectedMethod(method.to_string())),
        },

        CLASS_REQUEST => match method {
            METHOD_ALLOCATE => Ok(MessageHandler::AllocateRequest),
            METHOD_REFRESH => Ok(MessageHandler::RefreshRequest),
            METHOD_CREATE_PERMISSION => Ok(MessageHandler::CreatePermissionRequest),
            METHOD_CHANNEL_BIND => Ok(MessageHandler::ChannelBindRequest),
            METHOD_BINDING => Ok(MessageHandler::BindingRequest),
            // Not here: METHOD_CONNECT and METHOD_CONNECTION_BIND (RFC 6062), see the header.
            _ => Err(Error::UnexpectedMethod(method.to_string())),
        },

        _ => Err(Error::UnexpectedClass(class.to_string())),
    }
}

// pion/stun's `AttrType.Known()`: the attributes it has names for.
pub(crate) fn known(t: AttrType) -> bool {
    matches!(
        t,
        ATTR_MAPPED_ADDRESS
            | ATTR_USERNAME
            | ATTR_ERROR_CODE
            | ATTR_MESSAGE_INTEGRITY
            | ATTR_UNKNOWN_ATTRIBUTES
            | ATTR_REALM
            | ATTR_NONCE
            | ATTR_XORMAPPED_ADDRESS
            | ATTR_SOFTWARE
            | ATTR_ALTERNATE_SERVER
            | ATTR_FINGERPRINT
            | ATTR_PRIORITY
            | ATTR_USE_CANDIDATE
            | ATTR_ICE_CONTROLLED
            | ATTR_ICE_CONTROLLING
            | ATTR_CHANNEL_NUMBER
            | ATTR_LIFETIME
            | ATTR_XOR_PEER_ADDRESS
            | ATTR_DATA
            | ATTR_XOR_RELAYED_ADDRESS
            | ATTR_EVEN_PORT
            | ATTR_REQUESTED_TRANSPORT
            | ATTR_DONT_FRAGMENT
            | ATTR_RESERVATION_TOKEN
            | ATTR_CONNECTION_ID
            | ATTR_REQUESTED_ADDRESS_FAMILY
            | ATTR_MESSAGE_INTEGRITY_SHA256
            | ATTR_PASSWORD_ALGORITHM
            | ATTR_USER_HASH
            | ATTR_PASSWORD_ALGORITHMS
            | ATTR_ALTERNATE_DOMAIN
    ) || t.0 == 0xC070 // DTLS-IN-STUN
        || t.0 == 0xC071 // DTLS-IN-STUN-ACKNOWLEDGEMENT
}
