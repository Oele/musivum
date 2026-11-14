// Ported from pion/turn, internal/server/util.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust, on tokio (sending is async).
// - The attributes of a message are `Attr`s (`Arc`s of `stun::Setter`s), so that a response's
//   attributes can be kept (the response cache) and a message's reused.
// - `authenticateRequest` gives `Ok(Some((key, userID)))` when the request is authenticated,
//   `Ok(None)` when it answered the request instead (with a nonce, or 400 without an auth
//   handler) and `Err` with what failed otherwise: the original's `hasAuth` and `err`. The
//   message integrity is its key; `message_integrity` makes the attribute of it. The check is
//   made on a copy of the message (the WebRTC library's check changes the message while it
//   runs).
// - MESSAGE-INTEGRITY is computed by the WebRTC library's STUN crate with its default
//   cryptography (ring), made once for the process.

use std::sync::{Arc, LazyLock};

use super::{Error, Request};
use crate::go::time::{Duration, HOUR};
use crate::webrtc::stun::message::{
    CLASS_ERROR_RESPONSE, Getter, Message, MessageType, Method, Setter, TransactionId,
};
use crate::webrtc::turn::internal::auth::RequestAttributes;
use crate::webrtc::turn::internal::proto::{self, Lifetime, PROTO_UDP, RequestedTransport};
use crate::webrtc::turn::net::{Addr, PacketConn, is_closed};
use rtc::crypto::{RTCCrypto, RTCCryptoProvider};
use rtc::stun::attributes::{ATTR_MESSAGE_INTEGRITY, ATTR_NONCE, ATTR_REALM, ATTR_USERNAME};
use rtc::stun::error_code::{
    CODE_BAD_REQUEST, CODE_STALE_NONCE, CODE_UNAUTHORIZED, ErrorCode, ErrorCodeAttribute,
};
use rtc::stun::integrity::MessageIntegrity;
use rtc::stun::textattrs::TextAttribute;

// See: https://tools.ietf.org/html/rfc5766#section-6.2 defines 3600 seconds recommendation.
pub(crate) const MAXIMUM_ALLOCATION_LIFETIME: Duration = HOUR;

/// An attribute of a message to build: pion's `stun.Setter`.
pub type Attr = Arc<dyn Setter + Send + Sync>;

static CRYPTO_PROVIDER: LazyLock<Arc<dyn RTCCryptoProvider>> = LazyLock::new(|| {
    rtc::crypto::default_provider().expect("the WebRTC library is built with its cryptography")
});

/// The cryptography MESSAGE-INTEGRITY is computed with.
pub(crate) fn crypto() -> &'static dyn RTCCrypto {
    CRYPTO_PROVIDER.crypto()
}

/// `stun.MessageIntegrity(key)` as an attribute.
pub(crate) fn message_integrity(key: &[u8]) -> Attr {
    Arc::new(MessageIntegrity::new_raw_integrity_with_provider(
        key.to_vec(),
        crypto(),
    ))
}

/// `&stun.ErrorCodeAttribute{Code: code}`: the code without a reason.
pub(crate) fn error_code(code: ErrorCode) -> Attr {
    Arc::new(ErrorCodeAttribute {
        code,
        reason: Vec::new(),
    })
}

pub(crate) async fn build_and_send(
    conn: &dyn PacketConn,
    dst: Addr,
    attrs: Vec<Attr>,
) -> Result<(), Error> {
    let msg = {
        let setters: Vec<&dyn Setter> = attrs.iter().map(|a| a.as_ref() as &dyn Setter).collect();
        proto::build(&setters)?
    };
    match conn.write_to(&msg.raw, dst).await {
        Err(err) if is_closed(&err) => Ok(()),
        Err(err) => Err(err.into()),
        Ok(_) => Ok(()),
    }
}

// Send a STUN packet and return the original error to the caller.
pub(crate) async fn build_and_send_err(
    conn: &dyn PacketConn,
    dst: Addr,
    err: Error,
    attrs: Vec<Attr>,
) -> Error {
    if let Err(send_err) = build_and_send(conn, dst, attrs).await {
        return Error::FailedToSendError(send_err.to_string(), err.to_string());
    }

    err
}

pub(crate) fn build_msg(
    transaction_id: TransactionId,
    msg_type: MessageType,
    additional: Vec<Attr>,
) -> Vec<Attr> {
    let mut attrs: Vec<Attr> = vec![Arc::new(transaction_id), Arc::new(msg_type)];
    attrs.extend(additional);
    attrs
}

pub(crate) async fn authenticate_request(
    req: &Request<'_>,
    stun_msg: &Message,
    calling_method: Method,
) -> Result<Option<(Vec<u8>, String)>, Error> {
    let respond_with_nonce =
        async |response_code: ErrorCode| -> Result<Option<(Vec<u8>, String)>, Error> {
            let nonce = req.nonce_hash.generate()?;

            build_and_send(
                req.conn.as_ref(),
                req.src_addr,
                build_msg(
                    stun_msg.transaction_id,
                    MessageType::new(calling_method, CLASS_ERROR_RESPONSE),
                    vec![
                        error_code(response_code),
                        Arc::new(TextAttribute::new(ATTR_NONCE, nonce)),
                        Arc::new(TextAttribute::new(ATTR_REALM, req.realm.clone())),
                    ],
                ),
            )
            .await?;

            Ok(None)
        };

    if !stun_msg.contains(ATTR_MESSAGE_INTEGRITY) {
        return respond_with_nonce(CODE_UNAUTHORIZED).await;
    }

    let bad_request_msg = build_msg(
        stun_msg.transaction_id,
        MessageType::new(calling_method, CLASS_ERROR_RESPONSE),
        vec![error_code(CODE_BAD_REQUEST)],
    );

    // No Auth handler is set, server is running in STUN only mode
    // Respond with 400 so clients don't retry.
    let Some(auth_handler) = &req.auth_handler else {
        build_and_send(req.conn.as_ref(), req.src_addr, bad_request_msg).await?;

        return Ok(None);
    };

    let nonce_attr = match TextAttribute::get_from_as(stun_msg, ATTR_NONCE) {
        Ok(a) => a,
        Err(err) => {
            return Err(build_and_send_err(
                req.conn.as_ref(),
                req.src_addr,
                err.into(),
                bad_request_msg,
            )
            .await);
        }
    };

    // Assert Nonce is signed and is not expired.
    if req.nonce_hash.validate(&nonce_attr.text).is_err() {
        return respond_with_nonce(CODE_STALE_NONCE).await;
    }

    let realm_attr = match TextAttribute::get_from_as(stun_msg, ATTR_REALM) {
        Ok(a) => a,
        Err(err) => {
            return Err(build_and_send_err(
                req.conn.as_ref(),
                req.src_addr,
                err.into(),
                bad_request_msg,
            )
            .await);
        }
    };
    let username_attr = match TextAttribute::get_from_as(stun_msg, ATTR_USERNAME) {
        Ok(a) => a,
        Err(err) => {
            return Err(build_and_send_err(
                req.conn.as_ref(),
                req.src_addr,
                err.into(),
                bad_request_msg,
            )
            .await);
        }
    };

    let (user_id, our_key, ok) = auth_handler(&RequestAttributes {
        username: username_attr.text.clone(),
        realm: realm_attr.text.clone(),
        src_addr: req.src_addr,
        tls: req.tls.clone(),
        method: calling_method,
    });
    if !ok {
        return Err(build_and_send_err(
            req.conn.as_ref(),
            req.src_addr,
            Error::NoSuchUser(username_attr.text.clone()),
            bad_request_msg,
        )
        .await);
    }

    let mut checked = stun_msg.clone();
    if let Err(err) = MessageIntegrity::check(&mut checked, &our_key, crypto()) {
        gen_auth_event(req, stun_msg, calling_method, false);

        return Err(build_and_send_err(
            req.conn.as_ref(),
            req.src_addr,
            err.into(),
            bad_request_msg,
        )
        .await);
    }

    gen_auth_event(req, stun_msg, calling_method, true);

    Ok(Some((our_key, user_id)))
}

fn gen_auth_event(req: &Request<'_>, stun_msg: &Message, calling_method: Method, verdict: bool) {
    let Some(on_auth) = &req.allocation_manager.event_handler.on_auth else {
        return;
    };

    let Ok(realm_attr) = TextAttribute::get_from_as(stun_msg, ATTR_REALM) else {
        return;
    };

    // Auth event is generated per the username, not the user-id.
    let Ok(username_attr) = TextAttribute::get_from_as(stun_msg, ATTR_USERNAME) else {
        return;
    };

    let mut transport_attr = RequestedTransport::default();
    if transport_attr.get_from(stun_msg).is_err() {
        transport_attr = RequestedTransport {
            protocol: PROTO_UDP,
        };
    }

    on_auth(
        Some(req.src_addr),
        Some(req.conn.local_addr()),
        &transport_attr.protocol.to_string(),
        &username_attr.text,
        &realm_attr.text,
        &calling_method.to_string(),
        verdict,
    );
}

pub(crate) fn allocation_life_time(req: &Request<'_>, m: &Message) -> Duration {
    let mut lifetime_duration = req.allocation_lifetime;

    let mut lifetime = Lifetime::default();
    if lifetime.get_from(m).is_ok() {
        let d = Duration::from_secs(lifetime.0.as_secs() as i64);
        if d < MAXIMUM_ALLOCATION_LIFETIME {
            lifetime_duration = d;
        }
    }

    lifetime_duration
}
