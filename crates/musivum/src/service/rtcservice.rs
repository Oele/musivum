// Ported from LiveKit, pkg/service/rtcservice.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, for any HTTP server (see `service/mod.rs`).
// - The handlers are functions over the `http` crate's `request::Parts`, which carry the
//   grants `ApiKeyAuthMiddleware` put there and the `RemoteAddr`. `v0_validate` and
//   `v1_validate` return the response. `v0` and `v1` are async (they wait for the first
//   response of the session, as the original does before upgrading) and return either the
//   response the original writes without upgrading or an `RtcUpgrade`: the server upgrades
//   the request and awaits `RtcUpgrade::run` with its socket (a `SignalWebSocket`), which is
//   the rest of `serve`. `SetupRoutes` has no counterpart: the server routes `/rtc`,
//   `/rtc/validate`, `/rtc/v1` and `/rtc/v1/validate` to them.
// - The upgrader is the server's. What gorilla's `Upgrader.Upgrade` refuses a request for
//   (the method, the version, the key; the origin is always allowed here) is checked where
//   the original upgrades, and refused with gorilla's answer (`check_upgrade`); negotiating
//   compression (`EnableCompression`) is left to the server, and so is the bound on a frame
//   (`conn.SetReadLimit`), which `RtcUpgrade::read_limit` gives.
// - The router is a `routing::Router`, which upstream's is on every node (the wiring hands
//   the router in), so `ValidateConnectRequest`'s `router.(routing.Router)` holds: it is given
//   the node the router has for the room named in the token, the only room name the
//   validation finds here.
// - `config.SignalRelay.ConnectAttempts` is its default, 3 (`signal_relay` is not ported).
// - The logger has no deferred values (`WithDeferredValues`): `ServeLogger` keeps the fields
//   and gives the logger with them once they are resolved; a line before that is written
//   without them where upstream holds it back until they are. Upstream the fields are local
//   variables of `serve` that the response pump changes as well, without a lock; here they
//   are behind one.
// - `prometheus.RecordSessionJoinLatency` is not ported yet; the call is a comment at its
//   place.
// - No context: `utils.ContextWithAttempt` has no counterpart, nor has a cancelled request
//   (`context.Canceled`); a request that goes away drops the future, and a session started
//   for it is closed (`CloseOnDrop`, `ServeCleanup`).
// - The deferred function of `serve` is `ServeCleanup`: `RtcUpgrade::run` ends with it, and it
//   runs without waiting when an upgrade is dropped without being run or `run` is dropped.
// - `connections` is keyed by a number of the socket's; `DrainConnections` is async.
//   `drainMessageSource` takes a `write` that returns a future.
// - `ParticipantInit.reconnect_reason` is the protocol's enum: a reason the protocol does not
//   know is `RR_UNKNOWN`, where upstream keeps the number.
// The errors of the file are in errors.rs.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use base64::Engine as _;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::request::Parts;
use http::{Method, StatusCode};
use musivum_protocol::Message as _;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::auth::header_value;
use super::{
    Error, ErrorResponse, RoomAllocator, SignalWebSocket, ValidateConnectRequestParams,
    WsSignalConnection, augment_client_info, bool_value, decompress_gzip, form_value, get_api_key,
    get_grants, handle_error, is_web_socket_close_error, parse_client_info,
    validate_connect_request,
};
use crate::config::{Config, LimitConfig};
use crate::go::atomic;
use crate::go::time::{self, Duration, Time};
use crate::protocol::livekit;
use crate::protocol::livekit::{signal_request, signal_response, wrapped_join_request};
use crate::protocol::logger::{self, Logger};
use crate::routing::{
    Message, MessageSink, MessageSource, ParticipantInit, Router, StartParticipantSignalResults,
};
use crate::rtc::{self, BytesSignalStats};
use crate::telemetry::TelemetryService;
use crate::telemetry::prometheus;
use crate::{debugw, errorw, warnw};

// how long the response source is drained after the request direction is gone,
// applies only when the source is not closed by the relay, i. e. when there is
// no way to tell that everything pending has been read
const RESPONSE_FLUSH_TIMEOUT: Duration = Duration::from_millis(250);

// how long the response pump is given to stop, a bit more than the drain deadline
// so that it can finish the write it is in when that deadline expires
const RESPONSE_PUMP_DONE_TIMEOUT: Duration = Duration::from_millis(2 * 250);

// config.SignalRelay.ConnectAttempts, at its default
const CONNECT_ATTEMPTS: i64 = 3;

// Go's http.DefaultMaxHeaderBytes
const DEFAULT_MAX_HEADER_BYTES: usize = 1 << 20;

pub struct RtcService {
    router: Arc<dyn Router>,
    room_allocator: Arc<dyn RoomAllocator>,
    // upgrader: the server's (see the header)
    // read only for `signal_relay`, which is not ported
    #[allow(dead_code)]
    config: Arc<Config>,
    // not read, as upstream
    #[allow(dead_code)]
    is_dev: bool,
    limits: LimitConfig,
    telemetry: Arc<dyn TelemetryService>,

    connections: Arc<Connections>,
}

// `mu` and `connections` of the original, which an upgrade holds on to.
#[derive(Default)]
struct Connections {
    mu: Mutex<HashMap<u64, Arc<dyn SignalWebSocket>>>,
    next_id: AtomicU64,
}

impl RtcService {
    pub fn new(
        conf: Arc<Config>,
        ra: Arc<dyn RoomAllocator>,
        router: Arc<dyn Router>,
        telemetry: Arc<dyn TelemetryService>,
    ) -> Arc<RtcService> {
        // The upgrader of the original allows connections from any origin, since script may be
        // hosted anywhere; security is enforced by access tokens. See `check_upgrade`.
        Arc::new(RtcService {
            router,
            room_allocator: ra,
            is_dev: conf.development,
            limits: conf.limit.clone(),
            config: conf,
            telemetry,
            connections: Arc::new(Connections::default()),
        })
    }

    // SetupRoutes: no counterpart (see the header).

    /// `/rtc/validate`.
    pub fn v0_validate(&self, r: &Parts) -> http::Response<Bytes> {
        let lgr = logger::get_logger();
        let (_, _, code, err) = self.validate_internal(&lgr, r, false, true);
        if let Err(err) = err {
            return error_response(handle_error(r, code, &err, &[]));
        }
        text_response(StatusCode::OK, "success".to_string())
    }

    /// `/rtc/v1/validate`.
    pub fn v1_validate(&self, r: &Parts) -> http::Response<Bytes> {
        let lgr = logger::get_logger();
        let (_, _, code, err) = self.validate_internal(&lgr, r, true, true);
        if let Err(err) = err {
            return error_response(handle_error(r, code, &err, &[]));
        }
        text_response(StatusCode::OK, "success".to_string())
    }

    fn validate_internal(
        &self,
        lgr: &Logger,
        r: &Parts,
        needs_join_request: bool,
        strict: bool,
    ) -> (
        livekit::RoomName,
        ParticipantInit,
        StatusCode,
        Result<(), Error>,
    ) {
        let fail = |code: StatusCode, err: Error| {
            (
                livekit::RoomName::default(),
                ParticipantInit::default(),
                code,
                Err(err),
            )
        };

        if get_grants(&r.extensions).is_none_or(|claims| claims.video.is_none()) {
            return fail(
                StatusCode::UNAUTHORIZED,
                rtc::Error::PermissionDenied.into(),
            );
        }

        let mut params = ValidateConnectRequestParams::default();
        let mut use_single_peer_connection = false;
        let mut join_request = livekit::JoinRequest::default();

        let wrapped_join_request_base64 = form_value(r, "join_request");
        if wrapped_join_request_base64.is_empty() {
            if needs_join_request {
                return fail(StatusCode::BAD_REQUEST, Error::JoinRequestRequired);
            }

            params.publish = form_value(r, "publish");

            let attributes_str_param = form_value(r, "attributes");
            if !attributes_str_param.is_empty() {
                let attrs = match decode_attributes(&attributes_str_param) {
                    Ok(attrs) => attrs,
                    Err(err) => {
                        if strict {
                            return fail(StatusCode::BAD_REQUEST, Error::CannotDecodeAttributes);
                        }
                        debugw!(lgr, "failed to decode attributes", "error" => err);
                        // attrs will be empty here, so just proceed
                        HashMap::new()
                    }
                };
                params.attributes = attrs;
            }
        } else {
            use_single_peer_connection = true;
            let Ok(wrapped_proto_bytes) = url_encoding_decode(&wrapped_join_request_base64) else {
                return fail(
                    StatusCode::BAD_REQUEST,
                    Error::CannotBase64DecodeWrappedJoinRequest,
                );
            };
            let Ok(wrapped_join_request) =
                livekit::WrappedJoinRequest::decode(wrapped_proto_bytes.as_slice())
            else {
                return fail(
                    StatusCode::BAD_REQUEST,
                    Error::CannotUnmarshalWrappedJoinRequest,
                );
            };

            const NONE: i32 = wrapped_join_request::Compression::None as i32;
            const GZIP: i32 = wrapped_join_request::Compression::Gzip as i32;
            match wrapped_join_request.compression {
                NONE => {
                    if wrapped_join_request.join_request.len() > DEFAULT_MAX_HEADER_BYTES {
                        return fail(StatusCode::BAD_REQUEST, Error::JoinRequestTooLarge);
                    }
                    match livekit::JoinRequest::decode(wrapped_join_request.join_request.as_slice())
                    {
                        Ok(decoded) => join_request = decoded,
                        Err(_) => {
                            return fail(
                                StatusCode::BAD_REQUEST,
                                Error::CannotUnmarshalJoinRequest,
                            );
                        }
                    }
                }

                GZIP => {
                    let proto_bytes = match decompress_gzip(&wrapped_join_request.join_request) {
                        Ok(proto_bytes) => proto_bytes,
                        Err(err) => {
                            let err = match err {
                                Error::GzipTooLarge => Error::JoinRequestTooLarge,
                                Error::GzipReadFailed(_) => {
                                    Error::CannotReadDecompressedJoinRequest
                                }
                                err => err,
                            };
                            return fail(StatusCode::BAD_REQUEST, err);
                        }
                    };

                    match livekit::JoinRequest::decode(proto_bytes.as_slice()) {
                        Ok(decoded) => join_request = decoded,
                        Err(_) => {
                            return fail(
                                StatusCode::BAD_REQUEST,
                                Error::CannotUnmarshalJoinRequest,
                            );
                        }
                    }
                }

                // a compression the original does not know: neither case, and the join
                // request stays empty
                _ => {}
            }

            params.metadata = join_request.metadata.clone();
            params.attributes = join_request.participant_attributes.clone();
        }

        // `router.(routing.Router)`: the node of the room, which the validation takes the
        // region and the limits from
        let room_in_token = get_grants(&r.extensions)
            .and_then(|claims| claims.video.as_ref())
            .map(|video| livekit::RoomName::from(video.room.as_str()))
            .unwrap_or_default();
        let node = self.router.get_node_for_room(&room_in_token).ok();
        let (res, code, err) = validate_connect_request(
            lgr,
            r,
            &self.limits,
            params,
            node.as_ref(),
            self.room_allocator.as_ref(),
        );
        if let Err(err) = err {
            return (res.room_name, ParticipantInit::default(), code, Err(err));
        }

        let (identity, name) = match &res.grants {
            Some(grants) => (grants.identity.as_str(), grants.name.as_str()),
            None => ("", ""),
        };
        let mut pi = ParticipantInit {
            identity: livekit::ParticipantIdentity::from(identity),
            name: livekit::ParticipantName::from(name),
            grants: res.grants.clone(),
            api_key: get_api_key(&r.extensions),
            token_expires_at: res.token_expires_at,
            region: res.region,
            create_room: res.create_room_request,
            use_single_peer_connection,
            ..Default::default()
        };

        if wrapped_join_request_base64.is_empty() {
            pi.reconnect = bool_value(&form_value(r, "reconnect"));
            pi.client = Some(parse_client_info(r));

            pi.auto_subscribe = true;
            let auto_subscribe_param = form_value(r, "auto_subscribe");
            if !auto_subscribe_param.is_empty() {
                pi.auto_subscribe = bool_value(&auto_subscribe_param);
            }

            let auto_subscribe_data_track_param = form_value(r, "auto_subscribe_data_track");
            if !auto_subscribe_data_track_param.is_empty() {
                let auto_subscribe_data_track = bool_value(&auto_subscribe_data_track_param);
                pi.auto_subscribe_data_track = Some(auto_subscribe_data_track);
            }

            pi.adaptive_stream = bool_value(&form_value(r, "adaptive_stream"));
            pi.disable_ice_lite = bool_value(&form_value(r, "disable_ice_lite"));

            let reconnect_reason = atoi(&form_value(r, "reconnect_reason")); // 0 means unknown reason
            pi.reconnect_reason =
                livekit::ReconnectReason::try_from(reconnect_reason as i32).unwrap_or_default();

            if pi.reconnect {
                pi.id = livekit::ParticipantId::from(form_value(r, "sid"));
            }

            let subscriber_allow_pause_param = form_value(r, "subscriber_allow_pause");
            if !subscriber_allow_pause_param.is_empty() {
                let subscriber_allow_pause = bool_value(&subscriber_allow_pause_param);
                pi.subscriber_allow_pause = Some(subscriber_allow_pause);
            }
        } else {
            debugw!(lgr, "processing join request", "joinRequest" => &join_request);

            let mut client_info = join_request.client_info.take().unwrap_or_default();
            augment_client_info(&mut client_info, r);
            pi.client = Some(client_info);

            let cs = join_request.connection_settings.as_ref();
            pi.auto_subscribe = cs.is_some_and(|cs| cs.auto_subscribe);

            let auto_subscribe_data_track = cs
                .and_then(|cs| cs.auto_subscribe_data_track)
                .unwrap_or(false);
            pi.auto_subscribe_data_track = Some(auto_subscribe_data_track);

            pi.adaptive_stream = cs.is_some_and(|cs| cs.adaptive_stream);
            pi.disable_ice_lite = cs.is_some_and(|cs| cs.disable_ice_lite);

            if let Some(cs) = cs
                && let Some(v) = cs.subscriber_allow_pause
            {
                pi.subscriber_allow_pause = Some(v);
            }

            pi.add_track_requests = join_request.add_track_requests;
            pi.publisher_offer = join_request.publisher_offer;

            pi.reconnect = join_request.reconnect;
            pi.reconnect_reason = livekit::ReconnectReason::try_from(join_request.reconnect_reason)
                .unwrap_or_default();
            pi.id = livekit::ParticipantId::from(join_request.participant_sid);
        }

        (res.room_name, pi, code, Ok(()))
    }

    /// `/rtc`: the response to write, or the upgrade to run with the socket.
    // The response is what is answered to every request that is not upgraded: not an error
    // worth a box.
    #[allow(clippy::result_large_err)]
    pub async fn v0(&self, r: &Parts) -> Result<RtcUpgrade, http::Response<Bytes>> {
        self.serve(r, false).await
    }

    /// `/rtc/v1`: the response to write, or the upgrade to run with the socket.
    #[allow(clippy::result_large_err)]
    pub async fn v1(&self, r: &Parts) -> Result<RtcUpgrade, http::Response<Bytes>> {
        self.serve(r, true).await
    }

    #[allow(clippy::result_large_err)]
    async fn serve(
        &self,
        r: &Parts,
        needs_join_request: bool,
    ) -> Result<RtcUpgrade, http::Response<Bytes>> {
        // reject non websocket requests
        if !is_web_socket_upgrade(r) {
            return Err(empty_response(StatusCode::NOT_FOUND));
        }

        let started_at = time::now();
        let log = Arc::new(ServeLogger::new(logger::get_logger(), started_at));

        let (room_name, mut pi, code, err) =
            self.validate_internal(&log.logger(), r, needs_join_request, false);
        log.update(|f| f.room_name = room_name.clone());
        if let Err(err) = err {
            prometheus::increment_participant_join_validation_fail(1);
            log.resolve_logger(true);
            return Err(error_response(handle_error(
                r,
                code,
                &err,
                &log.get_logger_fields().kvs(),
            )));
        }

        log.update(|f| {
            f.participant_identity = pi.identity.clone();
            if !pi.id.is_empty() {
                f.p_id = pi.id.clone();
            }
        });
        {
            let f = log.get_logger_fields();
            debugw!(
                log.logger(),
                "join request validated",
                "room" => f.room_name,
                "roomID" => f.room_id,
                "participant" => f.participant_identity,
                "participantID" => f.p_id,
                "joinDuration" => f.join_duration,
                "participantInit" => &pi
            );
        }

        // give it a few attempts to start session
        let mut result = None;
        for attempt in 0..CONNECT_ATTEMPTS {
            let connection_timeout = Duration::from_secs(3 + attempt);
            let res = self
                .start_connection(&room_name, pi.clone(), connection_timeout)
                .await;
            let ok = res.is_ok();
            result = Some(res);
            if ok {
                break;
            }
        }

        let (cr, initial_response) = match result.expect("at least one attempt is made") {
            Ok(started) => started,
            Err(err) => {
                prometheus::increment_participant_join_fail(1);
                let status = err.to_http().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                log.resolve_logger(true);
                return Err(error_response(handle_error(
                    r,
                    status,
                    &err,
                    &log.get_logger_fields().kvs(),
                )));
            }
        };

        log.with_values(&[("connID", &cr.start_participant_signal_results.connection_id)]);
        let join = match &initial_response.message {
            Some(signal_response::Message::Join(join)) => Some(join),
            _ => None,
        };
        if !pi.reconnect
            && let Some(join) = join
        {
            let join_room_id = join
                .room
                .as_ref()
                .map(|room| room.sid.as_str())
                .unwrap_or("");
            if !join_room_id.is_empty() {
                log.update(|f| f.room_id = livekit::RoomId::from(join_room_id));
            }

            pi.id = livekit::ParticipantId::from(
                join.participant
                    .as_ref()
                    .map(|p| p.sid.as_str())
                    .unwrap_or(""),
            );
            log.update(|f| f.p_id = pi.id.clone());

            log.resolve_logger(false);
        }

        let signal_stats = BytesSignalStats::new(self.telemetry.clone());
        if let Some(join) = join {
            signal_stats.resolve_room(join.room.as_ref());
            signal_stats.resolve_participant(join.participant.as_ref());
        }
        if pi.reconnect && !pi.id.is_empty() {
            signal_stats.resolve_participant(Some(&livekit::ParticipantInfo {
                sid: pi.id.to_string(),
                identity: pi.identity.to_string(),
                ..Default::default()
            }));
        }

        let closed_by_client = Arc::new(atomic::Bool::new(false));
        let done = CancellationToken::new();
        // function exits when websocket terminates, it'll close the event reading off of request sink and response source as well
        let cleanup = ServeCleanup {
            log: log.clone(),
            closed_by_client: closed_by_client.clone(),
            done: done.clone(),
            request_sink: cr.request_sink(),
            response_source: cr.response_source(),
            response_pump_done: None,
            sig_conn: None,
            signal_stats: signal_stats.clone(),
            finished: false,
        };

        // upgrade only once the basics are good to go
        if let Err((status, err)) = check_upgrade(r) {
            prometheus::increment_participant_join_upgrade_fail(1);
            log.resolve_logger(true);
            // gorilla has answered the request with the status of the refusal; HandleError's
            // status comes too late and its text is added to gorilla's
            let written = handle_error(
                r,
                StatusCode::INTERNAL_SERVER_ERROR,
                &err,
                &log.get_logger_fields().kvs(),
            );
            return Err(handshake_error_response(status, &written.body));
        }

        Ok(RtcUpgrade {
            connections: self.connections.clone(),
            signal_message_size_limit: self.limits.signal_message_size_limit,
            log,
            pi,
            cr,
            initial_response,
            signal_stats,
            closed_by_client,
            done,
            cleanup,
        })
    }

    pub async fn drain_connections(&self, interval: Duration, force: bool) {
        let conns: Vec<Arc<dyn SignalWebSocket>> =
            self.connections.mu.lock().values().cloned().collect();

        if !force {
            // jitter drain start
            time::sleep(Duration::from_nanos(rand::random_range(
                0..interval.nanoseconds(),
            )))
            .await;

            let mut t = tokio::time::interval_at(
                tokio::time::Instant::now() + interval.to_std(),
                interval.to_std(),
            );

            for c in conns {
                let _ = c.close().await;
                t.tick().await;
            }
        } else {
            // drain as quickly as possible when forced
            for c in conns {
                let _ = c.close().await;
            }
        }
    }

    async fn start_connection(
        &self,
        room_name: &livekit::RoomName,
        pi: ParticipantInit,
        timeout: Duration,
    ) -> Result<(ConnectionResult, livekit::SignalResponse), Error> {
        self.room_allocator
            .select_room_node(room_name, &livekit::NodeId::default())?;

        // this needs to be started first *before* using router functions on this node
        let cr = ConnectionResult {
            start_participant_signal_results: self
                .router
                .start_participant_signal(room_name, pi)?,
            room: None,
        };

        // wait for the first message before upgrading to websocket. If no one is
        // responding to our connection attempt, we should terminate the connection
        // instead of waiting forever on the WebSocket
        let mut close_on_drop = CloseOnDrop(Some((cr.request_sink(), cr.response_source())));
        match read_initial_response(cr.response_source().as_ref(), timeout).await {
            // close the connection to avoid leaking: dropping `close_on_drop`
            Err(err) => Err(err),
            Ok(initial_response) => {
                close_on_drop.0 = None;
                Ok((cr, initial_response))
            }
        }
    }
}

fn decode_attributes(str: &str) -> Result<HashMap<String, String>, String> {
    let data = url_encoding_decode(str).map_err(|err| err.to_string())?;
    // `null` leaves Go's map nil, which is empty
    let attrs: Option<HashMap<String, String>> =
        serde_json::from_slice(&data).map_err(|err| err.to_string())?;
    Ok(attrs.unwrap_or_default())
}

// Go's base64.URLEncoding.DecodeString: the URL-safe alphabet with padding, which must be
// there; bits after the last byte need not be zero; line breaks are skipped.
fn url_encoding_decode(s: &str) -> Result<Vec<u8>, base64::DecodeError> {
    go_decode(&base64::alphabet::URL_SAFE, s)
}

// Go's base64.StdEncoding.DecodeString, the same with the standard alphabet.
fn std_encoding_decode(s: &str) -> Result<Vec<u8>, base64::DecodeError> {
    go_decode(&base64::alphabet::STANDARD, s)
}

fn go_decode(
    alphabet: &base64::alphabet::Alphabet,
    s: &str,
) -> Result<Vec<u8>, base64::DecodeError> {
    let engine = GeneralPurpose::new(
        alphabet,
        GeneralPurposeConfig::new()
            .with_decode_allow_trailing_bits(true)
            .with_decode_padding_mode(DecodePaddingMode::RequireCanonical),
    );
    let s: String = s.chars().filter(|c| *c != '\r' && *c != '\n').collect();
    engine.decode(s)
}

// Go's strconv.Atoi as the original reads its result: 0 when it is not a number, the
// largest or smallest int when it is one out of range.
fn atoi(s: &str) -> i64 {
    match s.parse::<i64>() {
        Ok(v) => v,
        Err(err) => match err.kind() {
            std::num::IntErrorKind::PosOverflow => i64::MAX,
            std::num::IntErrorKind::NegOverflow => i64::MIN,
            _ => 0,
        },
    }
}

/// What `v0` and `v1` hand the server when the request is to be upgraded: the rest of the
/// original's `serve`, after `upgrader.Upgrade`. Dropping it without running it ends the
/// session as the original does when the upgrade fails.
pub struct RtcUpgrade {
    connections: Arc<Connections>,
    signal_message_size_limit: i64,
    log: Arc<ServeLogger>,
    pi: ParticipantInit,
    cr: ConnectionResult,
    initial_response: livekit::SignalResponse,
    signal_stats: Arc<BytesSignalStats>,
    closed_by_client: Arc<atomic::Bool>,
    done: CancellationToken,
    cleanup: ServeCleanup,
}

impl fmt::Debug for RtcUpgrade {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RtcUpgrade")
            .field(
                "connectionID",
                &self.cr.start_participant_signal_results.connection_id,
            )
            .field("participantInit", &self.pi)
            .finish()
    }
}

impl RtcUpgrade {
    /// The bound on the size of a single signalling frame that the original sets on its
    /// socket (`conn.SetReadLimit`) when it is above 0, so that an oversized message is
    /// rejected by the transport before being fully buffered in memory. The server sets it
    /// on its socket; the decompressed size is bounded separately in `WsSignalConnection`.
    pub fn read_limit(&self) -> i64 {
        self.signal_message_size_limit
    }

    /// The session over the upgraded socket; returns when it has ended and the socket is
    /// closed.
    pub async fn run(self, conn: Arc<dyn SignalWebSocket>) {
        let RtcUpgrade {
            connections,
            signal_message_size_limit,
            log,
            pi,
            cr,
            initial_response,
            signal_stats,
            closed_by_client,
            done,
            mut cleanup,
        } = self;
        let request_sink = cr.request_sink();
        let response_source = cr.response_source();

        {
            let _connection = ConnectionEntry::add(&connections, conn.clone());

            async {
                // websocket established
                let sig_conn = WsSignalConnection::new(conn, signal_message_size_limit);
                cleanup.sig_conn = Some(sig_conn.clone());
                debugw!(log.logger(), "sending initial response", "response" => &initial_response);
                match sig_conn.write_response(&initial_response).await {
                    Err(err) => {
                        prometheus::increment_participant_join_write_initial_response_fail(1);
                        log.resolve_logger(true);
                        warnw!(log.logger(), "could not write initial response", err);
                        return;
                    }
                    Ok(count) => signal_stats.add_bytes(count as u64, true),
                }

                prometheus::increment_participant_join(1);
                log.update(|f| f.join_duration = time::since(log.started_at));
                // prometheus.RecordSessionJoinLatency(int(pi.Client.GetProtocol()), joinDuration):
                // not ported yet

                debugw!(
                    log.logger(),
                    "new client WS connected",
                    "reconnect" => pi.reconnect,
                    "reconnectReason" => pi.reconnect_reason,
                    "adaptiveStream" => pi.adaptive_stream,
                    "selectedNodeID" => cr.start_participant_signal_results.node_id,
                    "nodeSelectionReason" => cr.start_participant_signal_results.node_selection_reason
                );

                // handle responses
                let response_pump_done = CancellationToken::new();
                cleanup.response_pump_done = Some(response_pump_done.clone());
                let writer = Arc::new(ResponseWriter {
                    sig_conn: sig_conn.clone(),
                    log: log.clone(),
                    signal_stats: signal_stats.clone(),
                });
                let pump_logger = log.logger();
                let pump = response_pump(
                    writer,
                    response_source.clone(),
                    done.clone(),
                    closed_by_client.clone(),
                );
                let pump_sig_conn = sig_conn.clone();
                time::spawn(async move {
                    if rtc::recover_async(Some(&pump_logger), pump).await.is_err() {
                        std::process::exit(1);
                    }
                    response_pump_done.cancel();

                    // when the source is terminated, this means Participant.Close had been called and RTC connection is done
                    // we would terminate the signal connection as well
                    let _ = pump_sig_conn.close_with_reason("").await;
                });

                // handle incoming requests from websocket
                loop {
                    let (req, count) = match sig_conn.read_request().await {
                        Ok(read) => read,
                        Err(err) => {
                            if is_web_socket_close_error(&err) {
                                closed_by_client.store(true);
                            } else {
                                errorw!(log.logger(), "error reading from websocket", err);
                            }
                            return;
                        }
                    };
                    signal_stats.add_bytes(count as u64, false);

                    match &req.message {
                        Some(signal_request::Message::Ping(_)) => {
                            let pong = livekit::SignalResponse {
                                message: Some(signal_response::Message::Pong(
                                    //
                                    // Although this field is int64, some clients (like JS) cause overflow if nanosecond granularity is used.
                                    // So. use UnixMillis().
                                    //
                                    time::now().unix_milli(),
                                )),
                            };
                            if let Ok(count) = sig_conn.write_response(&pong).await {
                                signal_stats.add_bytes(count as u64, true);
                            }
                        }
                        Some(signal_request::Message::PingReq(ping_req)) => {
                            let pong = livekit::SignalResponse {
                                message: Some(signal_response::Message::PongResp(livekit::Pong {
                                    last_ping_timestamp: ping_req.timestamp,
                                    timestamp: time::now().unix_milli(),
                                })),
                            };
                            if let Ok(count) = sig_conn.write_response(&pong).await {
                                signal_stats.add_bytes(count as u64, true);
                            }
                        }
                        _ => {}
                    }

                    let l = log.logger();
                    match &req.message {
                        Some(signal_request::Message::Offer(_)) => {
                            debugw!(l, "received offer", "offer" => &req);
                        }
                        Some(signal_request::Message::Answer(_)) => {
                            debugw!(l, "received answer", "answer" => &req);
                        }
                        _ => {
                            debugw!(l, "received signal request", "request" => &req);
                        }
                    }

                    if let Err(err) = request_sink.write_message(req.into()) {
                        warnw!(log.logger(), "error writing to request sink", err);
                        return;
                    }
                }
            }
            .await;
        }

        cleanup.finish().await;
    }
}

// The pump of responses from the response source to the web socket: the goroutine of the
// original's `serve`, without its deferred functions, which its caller runs.
async fn response_pump(
    writer: Arc<ResponseWriter>,
    response_source: Arc<dyn MessageSource>,
    done: CancellationToken,
    closed_by_client: Arc<atomic::Bool>,
) {
    let read_chan = response_source.read_chan();
    loop {
        tokio::select! {
            _ = done.cancelled() => {
                // the request direction is gone, flush what the participant queued on its
                // way out unless the client is the one that went away
                if !closed_by_client.load()
                    && !drain_message_source(
                        response_source.as_ref(),
                        RESPONSE_FLUSH_TIMEOUT,
                        |msg| {
                            let writer = writer.clone();
                            async move { writer.write_response(msg).await }
                        },
                    )
                    .await
                {
                    debugw!(writer.log.logger(), "could not drain response source fully");
                }
                return;
            }

            msg = read_chan.recv() => {
                let Some(msg) = msg else {
                    writer.log.resolve_logger(true);
                    debugw!(writer.log.logger(), "nothing to read from response source");
                    return;
                };
                if !writer.write_response(msg).await {
                    return;
                }
            }
        }
    }
}

// `writeResponse` of the original's `serve`.
struct ResponseWriter {
    sig_conn: Arc<WsSignalConnection>,
    log: Arc<ServeLogger>,
    signal_stats: Arc<BytesSignalStats>,
}

impl ResponseWriter {
    // writes one response from the response source to the web socket,
    // returns false if the response pump should stop
    async fn write_response(&self, msg: Message) -> bool {
        let res = match msg {
            Message::SignalResponse(res) => res,
            msg => {
                errorw!(
                    self.log.logger(),
                    "unexpected message type",
                    nil,
                    "type" => msg.type_name()
                );
                return true;
            }
        };

        match &res.message {
            Some(signal_response::Message::Offer(_)) => {
                debugw!(self.log.logger(), "sending offer", "offer" => &res);
            }

            Some(signal_response::Message::Answer(_)) => {
                debugw!(self.log.logger(), "sending answer", "answer" => &res);
            }

            Some(signal_response::Message::Join(join)) => {
                debugw!(self.log.logger(), "sending join", "join" => &res);
                self.signal_stats.resolve_room(join.room.as_ref());
                self.signal_stats
                    .resolve_participant(join.participant.as_ref());
            }

            Some(signal_response::Message::RoomUpdate(room_update)) => {
                let update_room_id = room_update
                    .room
                    .as_ref()
                    .map(|room| room.sid.as_str())
                    .unwrap_or("");
                if !update_room_id.is_empty() {
                    self.log
                        .update(|f| f.room_id = livekit::RoomId::from(update_room_id));
                    self.log.resolve_logger(false);
                }
                debugw!(self.log.logger(), "sending room update", "roomUpdate" => &res);
                self.signal_stats.resolve_room(room_update.room.as_ref());
            }

            Some(signal_response::Message::Update(_)) => {
                debugw!(self.log.logger(), "sending participant update", "participantUpdate" => &res);
            }

            Some(signal_response::Message::RoomMoved(room_moved)) => {
                self.log.reset_logger();
                self.signal_stats.reset();

                let room = room_moved.room.as_ref();
                let participant = room_moved.participant.as_ref();
                self.log.update(|f| {
                    f.room_name =
                        livekit::RoomName::from(room.map(|r| r.name.as_str()).unwrap_or(""));
                    let move_room_id = room.map(|r| r.sid.as_str()).unwrap_or("");
                    if !move_room_id.is_empty() {
                        f.room_id = livekit::RoomId::from(move_room_id);
                    }
                    f.participant_identity = livekit::ParticipantIdentity::from(
                        participant.map(|p| p.identity.as_str()).unwrap_or(""),
                    );
                    f.p_id = livekit::ParticipantId::from(
                        participant.map(|p| p.sid.as_str()).unwrap_or(""),
                    );
                });
                self.log.resolve_logger(false);

                self.signal_stats.resolve_room(room);
                self.signal_stats.resolve_participant(participant);
                debugw!(self.log.logger(), "sending room moved", "roomMoved" => &res);
            }

            _ => {
                debugw!(self.log.logger(), "sending signal response", "response" => &res);
            }
        }

        match self.sig_conn.write_response(&res).await {
            Err(err) => {
                warnw!(self.log.logger(), "error writing to websocket", err);
                false
            }
            Ok(count) => {
                self.signal_stats.add_bytes(count as u64, true);
                true
            }
        }
    }
}

// drainMessageSource writes messages that are still queued in source using write.
// It is used when tearing a signalling connection down, responses the participant queued
// on its way out, a leave request on migration for example, would be dropped otherwise.
//
// The producer writes all pending messages into the source before closing it, so draining
// till the source is closed is a complete flush. The deadline is a backstop for the cases
// where the source is not closed, i. e. when there is no way to tell that everything
// pending has been read. Returns true if the source was drained fully.
async fn drain_message_source<F: Future<Output = bool>>(
    source: &dyn MessageSource,
    timeout: Duration,
    mut write: impl FnMut(Message) -> F,
) -> bool {
    let deadline = tokio::time::sleep(timeout.to_std());
    tokio::pin!(deadline);
    let read_chan = source.read_chan();

    loop {
        tokio::select! {
            msg = read_chan.recv() => {
                let Some(msg) = msg else {
                    return true;
                };
                if !write(msg).await {
                    return false;
                }
            }

            _ = &mut deadline => {
                return false;
            }
        }
    }
}

struct ConnectionResult {
    start_participant_signal_results: StartParticipantSignalResults,
    // never set, as upstream
    #[allow(dead_code)]
    room: Option<livekit::Room>,
}

impl ConnectionResult {
    // `cr.RequestSink`, which the router returns with every session it starts
    fn request_sink(&self) -> Arc<dyn MessageSink> {
        self.start_participant_signal_results
            .request_sink
            .clone()
            .expect("the router returns a request sink with a session")
    }

    // `cr.ResponseSource`, likewise
    fn response_source(&self) -> Arc<dyn MessageSource> {
        self.start_participant_signal_results
            .response_source
            .clone()
            .expect("the router returns a response source with a session")
    }
}

// Closes a session that `start_connection` started when it does not return it: the original
// does so when the first response does not come, and here also when the future is dropped
// while waiting for it.
struct CloseOnDrop(Option<(Arc<dyn MessageSink>, Arc<dyn MessageSource>)>);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        if let Some((request_sink, response_source)) = self.0.take() {
            request_sink.close();
            response_source.close();
        }
    }
}

async fn read_initial_response(
    source: &dyn MessageSource,
    timeout: Duration,
) -> Result<livekit::SignalResponse, Error> {
    match tokio::time::timeout(timeout.to_std(), source.read_chan().recv()).await {
        Err(_) => Err(Error::SignalResponseTimedOut),
        Ok(None) => Err(Error::ConnectionClosedByMedia),
        Ok(Some(Message::SignalResponse(res))) => Ok(*res),
        Ok(Some(msg)) => Err(Error::UnexpectedMessageType(msg.type_name().to_string())),
    }
}

// The deferred function of the original's `serve`, from where it is deferred (once the
// session is started) to the end of the session. `finish` is the function; dropping it
// unfinished does what of it can be done without waiting.
struct ServeCleanup {
    log: Arc<ServeLogger>,
    closed_by_client: Arc<atomic::Bool>,
    done: CancellationToken,
    request_sink: Arc<dyn MessageSink>,
    response_source: Arc<dyn MessageSource>,
    // closed by the response pump when it has stopped writing to the web socket; there once
    // the pump is started (`responsePumpStarted`)
    response_pump_done: Option<CancellationToken>,
    sig_conn: Option<Arc<WsSignalConnection>>,
    signal_stats: Arc<BytesSignalStats>,
    finished: bool,
}

impl ServeCleanup {
    async fn finish(mut self) {
        self.finished = true;
        self.log.resolve_logger(true);
        debugw!(
            self.log.logger(),
            "finishing WS connection",
            "closedByClient" => self.closed_by_client.load()
        );

        // signal the response pump before anything else so that it can flush responses
        // the participant queued on its way out, a leave request sent just before the
        // signalling connection was closed (on migration for example) is dropped otherwise
        self.done.cancel();
        self.request_sink.close();
        if let Some(response_pump_done) = &self.response_pump_done {
            tokio::select! {
                _ = response_pump_done.cancelled() => {}
                _ = time::sleep(RESPONSE_PUMP_DONE_TIMEOUT) => {
                    debugw!(self.log.logger(), "timed out waiting for response pump to finish");
                }
            }
        }
        self.response_source.close();

        // close the web socket on all paths, even when the response pump is wedged
        // writing to an unresponsive client
        if let Some(sig_conn) = &self.sig_conn {
            let _ = sig_conn.close_with_reason("").await;
        }

        self.signal_stats.stop();
    }
}

impl Drop for ServeCleanup {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.log.resolve_logger(true);
        debugw!(
            self.log.logger(),
            "finishing WS connection",
            "closedByClient" => self.closed_by_client.load()
        );
        self.done.cancel();
        self.request_sink.close();
        self.response_source.close();
        if let Some(sig_conn) = self.sig_conn.take() {
            time::spawn(async move {
                let _ = sig_conn.close_with_reason("").await;
            });
        }
        self.signal_stats.stop();
    }
}

// An entry of `connections` while the session runs on the socket: the original's deferred
// `delete(s.connections, conn)`.
struct ConnectionEntry {
    connections: Arc<Connections>,
    id: u64,
}

impl ConnectionEntry {
    fn add(connections: &Arc<Connections>, conn: Arc<dyn SignalWebSocket>) -> ConnectionEntry {
        let id = connections.next_id.fetch_add(1, Ordering::Relaxed);
        connections.mu.lock().insert(id, conn);
        ConnectionEntry {
            connections: connections.clone(),
            id,
        }
    }
}

impl Drop for ConnectionEntry {
    fn drop(&mut self) {
        self.connections.mu.lock().remove(&self.id);
    }
}

// The fields of the logger of `serve` (`getLoggerFields`): local variables of `serve`
// upstream.
#[derive(Clone, Default)]
struct LoggerFields {
    room_name: livekit::RoomName,
    room_id: livekit::RoomId,
    participant_identity: livekit::ParticipantIdentity,
    p_id: livekit::ParticipantId,
    join_duration: Duration,
}

impl LoggerFields {
    fn kvs(&self) -> [(&'static str, &dyn fmt::Debug); 5] {
        [
            ("room", &self.room_name),
            ("roomID", &self.room_id),
            ("participant", &self.participant_identity),
            ("participantID", &self.p_id),
            ("joinDuration", &self.join_duration),
        ]
    }
}

// `pLogger` and its resolver (`WithDeferredValues`), with the fields they are resolved with
// and `resolveLogger` and `resetLogger` of the original.
struct ServeLogger {
    started_at: Time,
    inner: Mutex<ServeLoggerInner>,
}

struct ServeLoggerInner {
    // pLogger without the deferred values
    base: Logger,
    // pLogger with them, once resolved
    resolved: Option<Logger>,
    fields: LoggerFields,
    logger_resolved: bool,
}

impl ServeLogger {
    fn new(base: Logger, started_at: Time) -> ServeLogger {
        ServeLogger {
            started_at,
            inner: Mutex::new(ServeLoggerInner {
                base,
                resolved: None,
                fields: LoggerFields::default(),
                logger_resolved: false,
            }),
        }
    }

    fn logger(&self) -> Logger {
        let inner = self.inner.lock();
        inner.resolved.clone().unwrap_or_else(|| inner.base.clone())
    }

    // `pLogger = pLogger.WithValues(...)`
    fn with_values(&self, kvs: &[(&str, &dyn fmt::Debug)]) {
        let mut inner = self.inner.lock();
        inner.base = inner.base.with_values(kvs);
        if inner.resolved.is_some() {
            inner.resolved = Some(inner.base.with_values(&inner.fields.kvs()));
        }
    }

    fn update(&self, f: impl FnOnce(&mut LoggerFields)) {
        f(&mut self.inner.lock().fields);
    }

    fn get_logger_fields(&self) -> LoggerFields {
        self.inner.lock().fields.clone()
    }

    fn resolve_logger(&self, force: bool) {
        let mut inner = self.inner.lock();
        if inner.logger_resolved {
            return;
        }

        if force {
            let f = &mut inner.fields;
            if f.room_name.is_empty() {
                f.room_name = "unresolved".into();
            }
            if f.room_id.is_empty() {
                f.room_id = "unresolved".into();
            }
            if f.participant_identity.is_empty() {
                f.participant_identity = "unresolved".into();
            }
            if f.p_id.is_empty() {
                f.p_id = "unresolved".into();
            }
            if f.join_duration.is_zero() {
                f.join_duration = time::since(self.started_at);
            }
        }

        let f = &inner.fields;
        if !f.room_name.is_empty()
            && !f.room_id.is_empty()
            && !f.participant_identity.is_empty()
            && !f.p_id.is_empty()
        {
            inner.logger_resolved = true;
            inner.resolved = Some(inner.base.with_values(&inner.fields.kvs()));
        }
    }

    fn reset_logger(&self) {
        let mut inner = self.inner.lock();
        inner.resolved = None;

        inner.fields.room_name = livekit::RoomName::default();
        inner.fields.room_id = livekit::RoomId::default();
        inner.fields.participant_identity = livekit::ParticipantIdentity::default();
        inner.fields.p_id = livekit::ParticipantId::default();
        inner.logger_resolved = false;
    }
}

// gorilla/websocket's IsWebSocketUpgrade
fn is_web_socket_upgrade(r: &Parts) -> bool {
    token_list_contains_value(&r.headers, "Connection", "upgrade")
        && token_list_contains_value(&r.headers, "Upgrade", "websocket")
}

// What gorilla/websocket's `Upgrader.Upgrade` refuses a request for, with the status it
// answers with, as the original's upgrader is set up: any origin is allowed and no response
// headers are given. Its checks of the connection that follow (the hijacking of it) are the
// server's.
fn check_upgrade(r: &Parts) -> Result<(), (StatusCode, Error)> {
    const BAD_HANDSHAKE: &str = "websocket: the client is not using the websocket protocol: ";
    let refuse = |status: StatusCode, reason: String| Err((status, Error::Handshake(reason)));

    if !token_list_contains_value(&r.headers, "Connection", "upgrade") {
        return refuse(
            StatusCode::BAD_REQUEST,
            format!("{BAD_HANDSHAKE}'upgrade' token not found in 'Connection' header"),
        );
    }

    if !token_list_contains_value(&r.headers, "Upgrade", "websocket") {
        return refuse(
            StatusCode::BAD_REQUEST,
            format!("{BAD_HANDSHAKE}'websocket' token not found in 'Upgrade' header"),
        );
    }

    if r.method != Method::GET {
        return refuse(
            StatusCode::METHOD_NOT_ALLOWED,
            format!("{BAD_HANDSHAKE}request method is not GET"),
        );
    }

    if !token_list_contains_value(&r.headers, "Sec-Websocket-Version", "13") {
        return refuse(
            StatusCode::BAD_REQUEST,
            "websocket: unsupported version: 13 not found in 'Sec-Websocket-Version' header"
                .to_string(),
        );
    }

    let challenge_key = header_value(&r.headers, "Sec-Websocket-Key");
    if !is_valid_challenge_key(&challenge_key) {
        return refuse(
            StatusCode::BAD_REQUEST,
            "websocket: not a websocket handshake: 'Sec-WebSocket-Key' header must be Base64 encoded value of 16-byte in length"
                .to_string(),
        );
    }

    Ok(())
}

// gorilla/websocket's isValidChallengeKey
fn is_valid_challenge_key(s: &str) -> bool {
    // From RFC6455:
    //
    // A |Sec-WebSocket-Key| header field with a base64-encoded (see
    // Section 4 of [RFC4648]) value that, when decoded, is 16 bytes in
    // length.

    if s.is_empty() {
        return false;
    }
    std_encoding_decode(s).is_ok_and(|decoded| decoded.len() == 16)
}

// gorilla/websocket's tokenListContainsValue: whether one of the comma separated tokens of
// one of the header's values is `value`, ignoring ASCII case; a value that is not such a list
// is passed over from where it stops being one.
fn token_list_contains_value(headers: &http::HeaderMap, name: &str, value: &str) -> bool {
    'headers: for s in headers.get_all(name) {
        let mut s = s.as_bytes();
        loop {
            let t;
            (t, s) = next_token(skip_space(s));
            if t.is_empty() {
                continue 'headers;
            }
            s = skip_space(s);
            if !s.is_empty() && s[0] != b',' {
                continue 'headers;
            }
            if t.eq_ignore_ascii_case(value.as_bytes()) {
                return true;
            }
            if s.is_empty() {
                continue 'headers;
            }
            s = &s[1..];
        }
    }
    false
}

// skipSpace returns a slice of the string s with all leading RFC 2616 linear
// whitespace removed.
fn skip_space(s: &[u8]) -> &[u8] {
    let i = s
        .iter()
        .position(|b| *b != b' ' && *b != b'\t')
        .unwrap_or(s.len());
    &s[i..]
}

// nextToken returns the leading RFC 2616 token of s and the string following
// the token.
fn next_token(s: &[u8]) -> (&[u8], &[u8]) {
    // Token octets per RFC 2616.
    let is_token_octet = |b: u8| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b);
    let i = s
        .iter()
        .position(|b| !is_token_octet(*b))
        .unwrap_or(s.len());
    (&s[..i], &s[i..])
}

// What Go's response writer sends for `w.WriteHeader(status)` alone.
fn empty_response(status: StatusCode) -> http::Response<Bytes> {
    let mut response = http::Response::new(Bytes::new());
    *response.status_mut() = status;
    response
}

// What Go's response writer sends for a status and a text: the type of the text is found by
// sniffing it, which for the texts written here is plain text.
fn text_response(status: StatusCode, body: String) -> http::Response<Bytes> {
    let mut response = http::Response::new(Bytes::from(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        http::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

// What `HandleError` writes.
fn error_response(e: ErrorResponse) -> http::Response<Bytes> {
    text_response(e.status, e.body)
}

// What gorilla/websocket's `returnError` writes (`http.Error` with the text of the status),
// followed by what `HandleError` writes after it: its status is too late, its text is added.
fn handshake_error_response(status: StatusCode, text: &str) -> http::Response<Bytes> {
    let body = format!("{}\n{}", status.canonical_reason().unwrap_or(""), text);
    let mut response = text_response(status, body);
    let headers = response.headers_mut();
    headers.insert(
        "Sec-Websocket-Version",
        http::HeaderValue::from_static("13"),
    );
    headers.insert(
        "X-Content-Type-Options",
        http::HeaderValue::from_static("nosniff"),
    );
    response
}

#[cfg(test)]
#[path = "rtcservice_test.rs"]
mod rtcservice_test;
