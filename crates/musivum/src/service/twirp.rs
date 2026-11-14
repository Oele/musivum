// Ported from LiveKit, pkg/service/twirp.go at v1.13.11; and the server of the room service
// from LiveKit's protocol, livekit/livekit_room.twirp.go (and the helpers of the package's
// generated code in cloud_replay.twirp.go) at v1.53.1-0.20261023175527-46332a036e25, which
// twitchtv/twirp's protoc-gen-twirp v8.1.3 generated, with what of Twirp v8.1.3 it uses
// (errors.go, server_options.go, context.go) and the server options of the protocol's
// utils/xtwirp.
// Copyright 2022 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Twirp: Copyright 2018 Twitch Interactive, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, for any HTTP server, as the rest of the package.
// - A request is the `http` crate's `request::Parts` and its body; `serve_http` gives the
//   response (`ServeHTTP` writes it). Its `context.Context` is the parts' extensions, cloned
//   (the auth middleware's grants are in them): Twirp's `ctxsetters` and the hooks keep their
//   values there, and the room service reads the grants from them. A hook takes the
//   extensions where the original takes and returns a context; its error is a Twirp error.
// - Twirp's types are here under names with `Twirp` (`TwirpError`, `TwirpErrorCode`,
//   `TwirpServerHooks`, `twirp_chain_hooks`, `twirp_service_name`, ...), only what the server
//   uses. An error's `meta` is kept sorted, as `encoding/json` writes a map.
// - The server always does what LiveKit's server options do (`xtwirp.DefaultServerOptions()`,
//   server.go): `ServerPassErrorDetails` turns an error of the service that is not a Twirp
//   error into one with the code its gRPC status gives (`to_error`). `ServerPassTimeout` has
//   nothing to do here (there are no deadlines on a request), nor `xtwirp.PassHeadersHandler`.
//   `twirp_error_as` is psrpc's conversion of its errors for `errors.As`, which the generated
//   `writeError` would use without those options.
// - A panic of the service is answered with Twirp's internal error and the error hook, as the
//   generated `ensurePanicResponses` does, and logged; it is not raised again (Go's HTTP server
//   recovers from it after the response is out, a Rust server would not answer).
// - The request is read whole by the server before; reading it cannot fail here. The JSON
//   form of the messages is that of `protojson.rs`.
// - `TwirpTelemetry`, `RecordRequest` and `RecordResponse` are not ported: LiveKit's server
//   does not install those hooks, so the calls of the room service fill in nothing there.
//   `TwirpEgressID` (egressid.go) is left out with egress. The logger's pool (`sync.Pool`) and
//   its deadlines (`requestedTimeout`, `modifiedTimeout`: a request has none) are not here;
//   `utils.GetLogger(ctx)` is the default logger.
// - Not ported of the generated code: the clients, `ServiceDescriptor` and
//   `ProtocGenTwirpVersion`, the interceptors (`twirp.Interceptor`, except LiveKit's, above).

use std::collections::BTreeMap;
use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;

use http::request::Parts;
use http::{Extensions, HeaderValue, Method, Response, StatusCode};
use parking_lot::Mutex;

use super::protojson::{self, MarshalOptions};
use super::{Error, RoomService, get_api_key};
use crate::errorw;
use crate::go::time::{self, Time};
use crate::protocol::livekit;
use crate::protocol::logger;
use crate::routing::PsrpcErrorCode;
use crate::telemetry::prometheus;
use crate::utils::COMPONENT_API;

// ------------------------------------------------
// Twirp: errors.go

/// Twirp's `ErrorCode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TwirpErrorCode {
    Canceled,
    Unknown,
    InvalidArgument,
    Malformed,
    DeadlineExceeded,
    NotFound,
    BadRoute,
    AlreadyExists,
    PermissionDenied,
    Unauthenticated,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
    NoError,
}

impl TwirpErrorCode {
    /// The code's text: Go's `string(code)`.
    pub fn as_str(self) -> &'static str {
        match self {
            TwirpErrorCode::Canceled => "canceled",
            TwirpErrorCode::Unknown => "unknown",
            TwirpErrorCode::InvalidArgument => "invalid_argument",
            TwirpErrorCode::Malformed => "malformed",
            TwirpErrorCode::DeadlineExceeded => "deadline_exceeded",
            TwirpErrorCode::NotFound => "not_found",
            TwirpErrorCode::BadRoute => "bad_route",
            TwirpErrorCode::AlreadyExists => "already_exists",
            TwirpErrorCode::PermissionDenied => "permission_denied",
            TwirpErrorCode::Unauthenticated => "unauthenticated",
            TwirpErrorCode::ResourceExhausted => "resource_exhausted",
            TwirpErrorCode::FailedPrecondition => "failed_precondition",
            TwirpErrorCode::Aborted => "aborted",
            TwirpErrorCode::OutOfRange => "out_of_range",
            TwirpErrorCode::Unimplemented => "unimplemented",
            TwirpErrorCode::Internal => "internal",
            TwirpErrorCode::Unavailable => "unavailable",
            TwirpErrorCode::DataLoss => "data_loss",
            TwirpErrorCode::NoError => "",
        }
    }
}

impl fmt::Display for TwirpErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Twirp's `ServerHTTPStatusFromErrorCode`.
pub fn server_http_status_from_error_code(code: TwirpErrorCode) -> StatusCode {
    match code {
        TwirpErrorCode::Canceled => StatusCode::REQUEST_TIMEOUT,
        TwirpErrorCode::Unknown => StatusCode::INTERNAL_SERVER_ERROR,
        TwirpErrorCode::InvalidArgument => StatusCode::BAD_REQUEST,
        TwirpErrorCode::Malformed => StatusCode::BAD_REQUEST,
        TwirpErrorCode::DeadlineExceeded => StatusCode::REQUEST_TIMEOUT,
        TwirpErrorCode::NotFound => StatusCode::NOT_FOUND,
        TwirpErrorCode::BadRoute => StatusCode::NOT_FOUND,
        TwirpErrorCode::AlreadyExists => StatusCode::CONFLICT,
        TwirpErrorCode::PermissionDenied => StatusCode::FORBIDDEN,
        TwirpErrorCode::Unauthenticated => StatusCode::UNAUTHORIZED,
        TwirpErrorCode::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        TwirpErrorCode::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
        TwirpErrorCode::Aborted => StatusCode::CONFLICT,
        TwirpErrorCode::OutOfRange => StatusCode::BAD_REQUEST,
        TwirpErrorCode::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        TwirpErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        TwirpErrorCode::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        TwirpErrorCode::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
        TwirpErrorCode::NoError => StatusCode::OK,
    }
}

/// Twirp's `Error`: a code, a message and metadata, which the server answers with as JSON.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TwirpError {
    code: TwirpErrorCode,
    msg: String,
    meta: BTreeMap<String, String>,
}

/// Twirp's `NewError`.
pub fn new_error(code: TwirpErrorCode, msg: &str) -> TwirpError {
    TwirpError {
        code,
        msg: msg.to_string(),
        meta: BTreeMap::new(),
    }
}

/// Twirp's `InvalidArgumentError`.
pub fn invalid_argument_error(argument: &str, validation_msg: &str) -> TwirpError {
    new_error(
        TwirpErrorCode::InvalidArgument,
        &format!("{argument} {validation_msg}"),
    )
    .with_meta("argument", argument)
}

/// Twirp's `InternalError`.
pub fn internal_error(msg: &str) -> TwirpError {
    new_error(TwirpErrorCode::Internal, msg)
}

impl TwirpError {
    pub fn code(&self) -> TwirpErrorCode {
        self.code
    }

    pub fn msg(&self) -> &str {
        &self.msg
    }

    pub fn with_meta(&self, key: &str, value: &str) -> TwirpError {
        let mut err = self.clone();
        err.meta.insert(key.to_string(), value.to_string());
        err
    }

    pub fn meta(&self, key: &str) -> &str {
        self.meta.get(key).map(String::as_str).unwrap_or("")
    }

    pub fn meta_map(&self) -> &BTreeMap<String, String> {
        &self.meta
    }
}

impl fmt::Display for TwirpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "twirp error {}: {}", self.code, self.msg)
    }
}

// ------------------------------------------------
// Twirp: context.go, the values the server keeps in a request's context

#[derive(Clone)]
struct PackageNameKey(&'static str);
#[derive(Clone)]
struct ServiceNameKey(&'static str);
#[derive(Clone)]
struct MethodNameKey(&'static str);
#[derive(Clone)]
struct StatusCodeKey(String);

/// Twirp's `PackageName(ctx)`.
pub fn twirp_package_name(ctx: &Extensions) -> Option<&str> {
    ctx.get::<PackageNameKey>().map(|k| k.0)
}

/// Twirp's `ServiceName(ctx)`.
pub fn twirp_service_name(ctx: &Extensions) -> Option<&str> {
    ctx.get::<ServiceNameKey>().map(|k| k.0)
}

/// Twirp's `MethodName(ctx)`.
pub fn twirp_method_name(ctx: &Extensions) -> Option<&str> {
    ctx.get::<MethodNameKey>().map(|k| k.0)
}

/// Twirp's `StatusCode(ctx)`: the status of the response, as text ("200").
pub fn twirp_status_code(ctx: &Extensions) -> Option<&str> {
    ctx.get::<StatusCodeKey>().map(|k| k.0.as_str())
}

// ------------------------------------------------
// Twirp: server_options.go, the hooks

type RequestHook = Box<dyn Fn(&mut Extensions) -> Result<(), TwirpError> + Send + Sync>;
type ResponsePreparedHook = Box<dyn Fn(&mut Extensions) + Send + Sync>;
type ResponseSentHook = Box<dyn Fn(&Extensions) + Send + Sync>;
type ErrorHook = Box<dyn Fn(&mut Extensions, &TwirpError) + Send + Sync>;

/// Twirp's `ServerHooks`.
#[derive(Default)]
pub struct TwirpServerHooks {
    pub request_received: Option<RequestHook>,
    pub request_routed: Option<RequestHook>,
    pub response_prepared: Option<ResponsePreparedHook>,
    pub response_sent: Option<ResponseSentHook>,
    pub error: Option<ErrorHook>,
}

/// Twirp's `ChainHooks`.
pub fn twirp_chain_hooks(hooks: Vec<TwirpServerHooks>) -> TwirpServerHooks {
    let hooks = Arc::new(hooks);
    TwirpServerHooks {
        request_received: Some(Box::new({
            let hooks = hooks.clone();
            move |ctx| {
                for h in hooks.iter() {
                    if let Some(f) = &h.request_received {
                        f(ctx)?;
                    }
                }
                Ok(())
            }
        })),
        request_routed: Some(Box::new({
            let hooks = hooks.clone();
            move |ctx| {
                for h in hooks.iter() {
                    if let Some(f) = &h.request_routed {
                        f(ctx)?;
                    }
                }
                Ok(())
            }
        })),
        response_prepared: Some(Box::new({
            let hooks = hooks.clone();
            move |ctx| {
                for h in hooks.iter() {
                    if let Some(f) = &h.response_prepared {
                        f(ctx);
                    }
                }
            }
        })),
        response_sent: Some(Box::new({
            let hooks = hooks.clone();
            move |ctx| {
                for h in hooks.iter() {
                    if let Some(f) = &h.response_sent {
                        f(ctx);
                    }
                }
            }
        })),
        error: Some(Box::new(move |ctx, twerr| {
            for h in hooks.iter() {
                if let Some(f) = &h.error {
                    f(ctx, twerr);
                }
            }
        })),
    }
}

// ------------------------------------------------
// twirp.go

#[derive(Clone, Default)]
struct TwirpRequestFields {
    service: String,
    method: String,
    error: Option<TwirpError>,
}

// --------------------------------------------------------------------------

// A value of a log field, written as it was when it was appended (Go keeps the value itself).
struct LogValue(String);

impl fmt::Debug for LogValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// what the original keeps under `twirpLoggerKey{}`
#[derive(Clone)]
struct TwirpLoggerKey(Arc<Mutex<TwirpLogger>>);

// logging handling inspired by https://github.com/bakins/twirpzap
// License: Apache-2.0
pub fn twirp_logger() -> TwirpServerHooks {
    TwirpServerHooks {
        request_received: Some(Box::new(logger_request_received)),
        request_routed: Some(Box::new(logger_request_routed)),
        error: Some(Box::new(logger_error_received)),
        response_sent: Some(Box::new(logger_response_sent)),
        ..Default::default()
    }
}

struct TwirpLogger {
    twirp_request_fields: TwirpRequestFields,

    fields: Vec<(&'static str, LogValue)>,
    started_at: Time,
}

pub fn append_log_fields(ctx: &Extensions, fields: &[(&'static str, &dyn fmt::Debug)]) {
    let Some(r) = ctx.get::<TwirpLoggerKey>() else {
        return;
    };

    r.0.lock()
        .fields
        .extend(fields.iter().map(|(k, v)| (*k, LogValue(format!("{v:?}")))));
}

fn logger_request_received(ctx: &mut Extensions) -> Result<(), TwirpError> {
    let mut r = TwirpLogger {
        twirp_request_fields: TwirpRequestFields::default(),
        fields: Vec::new(),
        started_at: time::now(),
    };

    if let Some(svc) = twirp_service_name(ctx) {
        r.twirp_request_fields.service = svc.to_string();
        r.fields.push(("service", LogValue(format!("{svc:?}"))));
    }

    let api_key = get_api_key(ctx);
    if !api_key.is_empty() {
        r.fields.push(("apiKey", LogValue(format!("{api_key:?}"))));
    }

    ctx.insert(TwirpLoggerKey(Arc::new(Mutex::new(r))));
    Ok(())
}

fn logger_request_routed(ctx: &mut Extensions) -> Result<(), TwirpError> {
    if let Some(meth) = twirp_method_name(ctx) {
        let Some(l) = ctx.get::<TwirpLoggerKey>() else {
            return Ok(());
        };
        let mut l = l.0.lock();
        l.twirp_request_fields.method = meth.to_string();
        l.fields.push(("method", LogValue(format!("{meth:?}"))));
    }

    Ok(())
}

fn logger_response_sent(ctx: &Extensions) {
    let Some(r) = ctx.get::<TwirpLoggerKey>() else {
        return;
    };
    let mut r = r.0.lock();

    let duration = time::since(r.started_at);
    r.fields
        .push(("duration", LogValue(format!("{duration:?}"))));

    if let Some(status) = twirp_status_code(ctx) {
        r.fields.push(("status", LogValue(format!("{status:?}"))));
    }
    if let Some(error) = r.twirp_request_fields.error.clone() {
        r.fields
            .push(("error", LogValue(format!("{:?}", error.msg()))));
        r.fields
            .push(("code", LogValue(format!("{:?}", error.code().as_str()))));
    }

    let (status_family, _) = twirp_response_status(ctx, r.twirp_request_fields.error.as_ref());
    let service_method = format!(
        "API {}.{}",
        r.twirp_request_fields.service, r.twirp_request_fields.method
    );
    prometheus::record_twirp_request_latency(
        &r.twirp_request_fields.service,
        &r.twirp_request_fields.method,
        duration,
        &status_family,
    );
    let kvs: Vec<(&str, &dyn fmt::Debug)> = r
        .fields
        .iter()
        .map(|(k, v)| (*k, v as &dyn fmt::Debug))
        .collect();
    logger::get_logger()
        .with_component(COMPONENT_API)
        .infow(&service_method, &kvs);
}

fn logger_error_received(ctx: &mut Extensions, e: &TwirpError) {
    let Some(r) = ctx.get::<TwirpLoggerKey>() else {
        return;
    };

    r.0.lock().twirp_request_fields.error = Some(e.clone());
}

// --------------------------------------------------------------------------

// what the original keeps under `statusReporterKey{}`
#[derive(Clone)]
struct StatusReporterKey(Arc<Mutex<TwirpRequestFields>>);

pub fn twirp_request_status_reporter() -> TwirpServerHooks {
    TwirpServerHooks {
        request_received: Some(Box::new(status_reporter_request_received)),
        request_routed: Some(Box::new(status_reporter_request_routed)),
        error: Some(Box::new(status_reporter_error_received)),
        response_sent: Some(Box::new(status_reporter_response_sent)),
        ..Default::default()
    }
}

fn status_reporter_request_received(ctx: &mut Extensions) -> Result<(), TwirpError> {
    let mut r = TwirpRequestFields::default();

    if let Some(svc) = twirp_service_name(ctx) {
        r.service = svc.to_string();
    }

    ctx.insert(StatusReporterKey(Arc::new(Mutex::new(r))));
    Ok(())
}

fn status_reporter_request_routed(ctx: &mut Extensions) -> Result<(), TwirpError> {
    if let Some(meth) = twirp_method_name(ctx) {
        let Some(l) = ctx.get::<StatusReporterKey>() else {
            return Ok(());
        };
        l.0.lock().method = meth.to_string();
    }

    Ok(())
}

fn twirp_response_status(ctx: &Extensions, err: Option<&TwirpError>) -> (String, TwirpErrorCode) {
    let mut status_family = String::new();
    let mut code = TwirpErrorCode::NoError;
    if let Some(status_code) = twirp_status_code(ctx) {
        status_family = http_status_family(status_code);
    }
    if let Some(err) = err {
        code = err.code();
    }
    (status_family, code)
}

fn http_status_family(status_code: &str) -> String {
    let mut status_family = String::new();
    if let Ok(status) = status_code.parse::<i64>() {
        status_family = match status {
            400..=499 => "4xx".to_string(),
            500..=599 => "5xx".to_string(),
            _ => status_code.to_string(),
        };
    }
    status_family
}

fn status_reporter_response_sent(ctx: &Extensions) {
    let Some(r) = ctx.get::<StatusReporterKey>() else {
        return;
    };
    let r = r.0.lock().clone();

    let (status_family, code) = twirp_response_status(ctx, r.error.as_ref());
    prometheus::record_twirp_request_status(&r.service, &r.method, &status_family, code.as_str());
}

fn status_reporter_error_received(ctx: &mut Extensions, e: &TwirpError) {
    let Some(r) = ctx.get::<StatusReporterKey>() else {
        return;
    };

    r.0.lock().error = Some(e.clone());
}

// TwirpTelemetry, RecordRequest and RecordResponse: not ported, see the description at the top.

// --------------------------------------------------------------------------
// The errors of the service as Twirp errors

/// The Twirp error that Go's `errors.As(err, &twerr)` finds in an error of the package: the
/// error itself where it is one, else the psrpc error in it as psrpc converts it
/// (`psrpcError.As`: the code by `ToTwirp`, the psrpc error's own text).
pub fn twirp_error_as(err: &Error) -> Option<TwirpError> {
    if let Error::Twirp(err) = err {
        return Some(err.clone());
    }
    let err = err.psrpc_error()?;
    Some(new_error(psrpc_code_to_twirp(err.code()), &err.to_string()))
}

/// LiveKit's `xtwirp.ToError`, which the server applies to an error of the service: a Twirp
/// error stays as it is; any other is one with the error's text and the code of its gRPC
/// status (that of the psrpc error in it, `ToGRPC`, back to Twirp's by
/// `xtwirp.ErrorCodeFromGRPC`), or `unknown` where it has none.
pub fn to_error(err: &Error) -> TwirpError {
    if let Error::Twirp(err) = err {
        return err.clone();
    }
    let e = new_error(TwirpErrorCode::Unknown, &err.to_string());
    match err.psrpc_error() {
        Some(st) => new_error(twirp_code_from_grpc_of(st.code()), e.msg()),
        None => e,
    }
}

// psrpc's `ErrorCode.ToTwirp`
fn psrpc_code_to_twirp(code: PsrpcErrorCode) -> TwirpErrorCode {
    match code {
        PsrpcErrorCode::Ok => TwirpErrorCode::NoError,
        PsrpcErrorCode::Canceled => TwirpErrorCode::Canceled,
        PsrpcErrorCode::Unknown => TwirpErrorCode::Unknown,
        PsrpcErrorCode::InvalidArgument => TwirpErrorCode::InvalidArgument,
        PsrpcErrorCode::MalformedRequest | PsrpcErrorCode::MalformedResponse => {
            TwirpErrorCode::Malformed
        }
        PsrpcErrorCode::DeadlineExceeded => TwirpErrorCode::DeadlineExceeded,
        PsrpcErrorCode::NotFound => TwirpErrorCode::NotFound,
        PsrpcErrorCode::AlreadyExists => TwirpErrorCode::AlreadyExists,
        PsrpcErrorCode::PermissionDenied => TwirpErrorCode::PermissionDenied,
        PsrpcErrorCode::ResourceExhausted => TwirpErrorCode::ResourceExhausted,
        PsrpcErrorCode::FailedPrecondition => TwirpErrorCode::FailedPrecondition,
        PsrpcErrorCode::Aborted => TwirpErrorCode::Aborted,
        PsrpcErrorCode::OutOfRange => TwirpErrorCode::OutOfRange,
        PsrpcErrorCode::Unimplemented => TwirpErrorCode::Unimplemented,
        PsrpcErrorCode::Internal => TwirpErrorCode::Internal,
        PsrpcErrorCode::Unavailable => TwirpErrorCode::Unavailable,
        PsrpcErrorCode::DataLoss => TwirpErrorCode::DataLoss,
        PsrpcErrorCode::Unauthenticated => TwirpErrorCode::Unauthenticated,
        PsrpcErrorCode::UpstreamServerError => TwirpErrorCode::Internal,
        PsrpcErrorCode::UpstreamClientError => TwirpErrorCode::InvalidArgument,
        PsrpcErrorCode::NotAcceptable | PsrpcErrorCode::UnprocessableEntity => {
            TwirpErrorCode::Unknown
        }
    }
}

// xtwirp's `ErrorCodeFromGRPC` of psrpc's `ErrorCode.ToGRPC`, the two in one
fn twirp_code_from_grpc_of(code: PsrpcErrorCode) -> TwirpErrorCode {
    match code {
        PsrpcErrorCode::Ok => TwirpErrorCode::NoError,
        PsrpcErrorCode::Canceled => TwirpErrorCode::Canceled,
        PsrpcErrorCode::Unknown => TwirpErrorCode::Unknown,
        PsrpcErrorCode::InvalidArgument | PsrpcErrorCode::MalformedRequest => {
            TwirpErrorCode::InvalidArgument
        }
        PsrpcErrorCode::DeadlineExceeded => TwirpErrorCode::DeadlineExceeded,
        PsrpcErrorCode::NotFound => TwirpErrorCode::NotFound,
        PsrpcErrorCode::AlreadyExists => TwirpErrorCode::AlreadyExists,
        PsrpcErrorCode::PermissionDenied => TwirpErrorCode::PermissionDenied,
        PsrpcErrorCode::ResourceExhausted => TwirpErrorCode::ResourceExhausted,
        PsrpcErrorCode::FailedPrecondition => TwirpErrorCode::FailedPrecondition,
        PsrpcErrorCode::Aborted => TwirpErrorCode::Aborted,
        PsrpcErrorCode::OutOfRange => TwirpErrorCode::OutOfRange,
        PsrpcErrorCode::Unimplemented => TwirpErrorCode::Unimplemented,
        PsrpcErrorCode::MalformedResponse | PsrpcErrorCode::Internal => TwirpErrorCode::Internal,
        PsrpcErrorCode::Unavailable => TwirpErrorCode::Unavailable,
        PsrpcErrorCode::DataLoss => TwirpErrorCode::DataLoss,
        PsrpcErrorCode::Unauthenticated => TwirpErrorCode::Unauthenticated,
        PsrpcErrorCode::UpstreamServerError => TwirpErrorCode::Internal,
        PsrpcErrorCode::UpstreamClientError => TwirpErrorCode::InvalidArgument,
        PsrpcErrorCode::NotAcceptable | PsrpcErrorCode::UnprocessableEntity => {
            TwirpErrorCode::Unknown
        }
    }
}

// --------------------------------------------------------------------------
// livekit_room.twirp.go: the server of the room service

/// What the server is made with: Twirp's `ServerOptions` as far as the generated server reads
/// them. `TwirpServerOptions::default()` is the default of each; `livekit_server_options()`
/// what LiveKit's server makes its Twirp servers with.
#[derive(Default)]
pub struct TwirpServerOptions {
    pub hooks: Option<TwirpServerHooks>,
    /// `twirp.WithServerPathPrefix`; "/twirp" where `None`.
    pub path_prefix: Option<String>,
    /// `twirp.WithServerJSONSkipDefaults`
    pub json_skip_defaults: bool,
    /// `twirp.WithServerJSONCamelCase`
    pub json_camel_case: bool,
}

/// The options of server.go: the hooks `TwirpLogger()` and `TwirpRequestStatusReporter()`
/// (`TwirpEgressID()` is left out with egress) and `xtwirp.DefaultServerOptions()`, which the
/// server always applies (see the description at the top).
pub fn livekit_server_options() -> TwirpServerOptions {
    TwirpServerOptions {
        hooks: Some(twirp_chain_hooks(vec![
            twirp_logger(),
            twirp_request_status_reporter(),
        ])),
        ..Default::default()
    }
}

pub struct RoomServiceServer {
    room_service: Arc<RoomService>,
    hooks: Option<TwirpServerHooks>,
    path_prefix: String,      // prefix for routing
    json_skip_defaults: bool, // do not include unpopulated fields (default values) in the response
    json_camel_case: bool, // JSON fields are serialized as lowerCamelCase rather than keeping the original proto names
}

// NewRoomServiceServer builds a TwirpServer that can be used as an http.Handler to handle
// HTTP requests that are routed to the right method in the provided svc implementation.
pub fn new_room_service_server(
    svc: Arc<RoomService>,
    opts: TwirpServerOptions,
) -> RoomServiceServer {
    RoomServiceServer {
        room_service: svc,
        hooks: opts.hooks,
        path_prefix: opts.path_prefix.unwrap_or_else(|| "/twirp".to_string()), // default prefix
        json_skip_defaults: opts.json_skip_defaults,
        json_camel_case: opts.json_camel_case,
    }
}

// RoomServicePathPrefix is a convenience constant that may identify URL paths.
// Should be used with caution, it only matches routes generated by Twirp Go clients,
// with the default "/twirp" prefix and default CamelCase service and method names.
// More info: https://twitchtv.github.io/twirp/docs/routing.html
pub const ROOM_SERVICE_PATH_PREFIX: &str = "/twirp/livekit.RoomService/";

// The methods of the service with their messages, in the order of the generated code.
type ServeFn = fn(&RoomServiceServer, &mut Extensions, &[u8], bool) -> Response<Vec<u8>>;

const METHODS: &[(&str, ServeFn)] = &[
    ("CreateRoom", |s, ctx, body, json| {
        s.serve(
            ctx,
            body,
            json,
            ("CreateRoom", "livekit.CreateRoomRequest", "livekit.Room"),
            |svc, ctx, r: &livekit::CreateRoomRequest| svc.create_room(ctx, r),
        )
    }),
    ("ListRooms", |s, ctx, body, json| {
        s.serve(
            ctx,
            body,
            json,
            (
                "ListRooms",
                "livekit.ListRoomsRequest",
                "livekit.ListRoomsResponse",
            ),
            |svc, ctx, r: &livekit::ListRoomsRequest| svc.list_rooms(ctx, r),
        )
    }),
    ("DeleteRoom", |s, ctx, body, json| {
        s.serve(
            ctx,
            body,
            json,
            (
                "DeleteRoom",
                "livekit.DeleteRoomRequest",
                "livekit.DeleteRoomResponse",
            ),
            |svc, ctx, r: &livekit::DeleteRoomRequest| svc.delete_room(ctx, r),
        )
    }),
    ("ListParticipants", |s, ctx, body, json| {
        s.serve(
            ctx,
            body,
            json,
            (
                "ListParticipants",
                "livekit.ListParticipantsRequest",
                "livekit.ListParticipantsResponse",
            ),
            |svc, ctx, r: &livekit::ListParticipantsRequest| svc.list_participants(ctx, r),
        )
    }),
    ("GetParticipant", |s, ctx, body, json| {
        s.serve(
            ctx,
            body,
            json,
            (
                "GetParticipant",
                "livekit.RoomParticipantIdentity",
                "livekit.ParticipantInfo",
            ),
            |svc, ctx, r: &livekit::RoomParticipantIdentity| svc.get_participant(ctx, r),
        )
    }),
    ("RemoveParticipant", |s, ctx, body, json| {
        s.serve(
            ctx,
            body,
            json,
            (
                "RemoveParticipant",
                "livekit.RoomParticipantIdentity",
                "livekit.RemoveParticipantResponse",
            ),
            |svc, ctx, r: &livekit::RoomParticipantIdentity| svc.remove_participant(ctx, r),
        )
    }),
    ("MutePublishedTrack", |s, ctx, body, json| {
        s.serve(
            ctx,
            body,
            json,
            (
                "MutePublishedTrack",
                "livekit.MuteRoomTrackRequest",
                "livekit.MuteRoomTrackResponse",
            ),
            |svc, ctx, r: &livekit::MuteRoomTrackRequest| svc.mute_published_track(ctx, r),
        )
    }),
    ("UpdateParticipant", |s, ctx, body, json| {
        s.serve(
            ctx,
            body,
            json,
            (
                "UpdateParticipant",
                "livekit.UpdateParticipantRequest",
                "livekit.ParticipantInfo",
            ),
            |svc, ctx, r: &livekit::UpdateParticipantRequest| svc.update_participant(ctx, r),
        )
    }),
    ("UpdateSubscriptions", |s, ctx, body, json| {
        s.serve(
            ctx,
            body,
            json,
            (
                "UpdateSubscriptions",
                "livekit.UpdateSubscriptionsRequest",
                "livekit.UpdateSubscriptionsResponse",
            ),
            |svc, ctx, r: &livekit::UpdateSubscriptionsRequest| svc.update_subscriptions(ctx, r),
        )
    }),
    ("SendData", |s, ctx, body, json| {
        s.serve(
            ctx,
            body,
            json,
            (
                "SendData",
                "livekit.SendDataRequest",
                "livekit.SendDataResponse",
            ),
            |svc, ctx, r: &livekit::SendDataRequest| svc.send_data(ctx, r),
        )
    }),
    ("UpdateRoomMetadata", |s, ctx, body, json| {
        s.serve(
            ctx,
            body,
            json,
            (
                "UpdateRoomMetadata",
                "livekit.UpdateRoomMetadataRequest",
                "livekit.Room",
            ),
            |svc, ctx, r: &livekit::UpdateRoomMetadataRequest| svc.update_room_metadata(ctx, r),
        )
    }),
    ("ForwardParticipant", |s, ctx, body, json| {
        s.serve(
            ctx,
            body,
            json,
            (
                "ForwardParticipant",
                "livekit.ForwardParticipantRequest",
                "livekit.ForwardParticipantResponse",
            ),
            |svc, ctx, r: &livekit::ForwardParticipantRequest| svc.forward_participant(ctx, r),
        )
    }),
    ("MoveParticipant", |s, ctx, body, json| {
        s.serve(
            ctx,
            body,
            json,
            (
                "MoveParticipant",
                "livekit.MoveParticipantRequest",
                "livekit.MoveParticipantResponse",
            ),
            |svc, ctx, r: &livekit::MoveParticipantRequest| svc.move_participant(ctx, r),
        )
    }),
    ("PerformRpc", |s, ctx, body, json| {
        s.serve(
            ctx,
            body,
            json,
            (
                "PerformRpc",
                "livekit.PerformRpcRequest",
                "livekit.PerformRpcResponse",
            ),
            |svc, ctx, r: &livekit::PerformRpcRequest| svc.perform_rpc(ctx, r),
        )
    }),
];

impl RoomServiceServer {
    /// `ServeHTTP`: the response to a request for the service, given its parts (with the
    /// grants of the auth middleware in their extensions) and its whole body.
    pub fn serve_http(&self, req: &Parts, body: &[u8]) -> Response<Vec<u8>> {
        let mut ctx = req.extensions.clone();
        ctx.insert(PackageNameKey("livekit"));
        ctx.insert(ServiceNameKey("RoomService"));

        if let Err(err) = call_request_received(&mut ctx, self.hooks.as_ref()) {
            return self.write_error(&mut ctx, err);
        }

        let path = url_path(req);
        if req.method != Method::POST {
            let msg = format!(
                "unsupported method {} (only POST is allowed)",
                go_quote(req.method.as_str())
            );
            return self.write_error(&mut ctx, bad_route_error(&msg, req.method.as_str(), &path));
        }

        // Verify path format: [<prefix>]/<package>.<Service>/<Method>
        let (prefix, pkg_service, method) = parse_twirp_path(&path);
        if pkg_service != "livekit.RoomService" {
            let msg = format!("no handler for path {}", go_quote(&path));
            return self.write_error(&mut ctx, bad_route_error(&msg, req.method.as_str(), &path));
        }
        if prefix != self.path_prefix {
            let msg = format!(
                "invalid path prefix {}, expected {}, on path {}",
                go_quote(&prefix),
                go_quote(&self.path_prefix),
                go_quote(&path)
            );
            return self.write_error(&mut ctx, bad_route_error(&msg, req.method.as_str(), &path));
        }

        match METHODS.iter().find(|(name, _)| *name == method) {
            Some((_, serve)) => {
                let header = header_get(req, "Content-Type");
                let i = header.find(';').unwrap_or(header.len());
                match header[..i].to_lowercase().trim() {
                    "application/json" => serve(self, &mut ctx, body, true),
                    "application/protobuf" => serve(self, &mut ctx, body, false),
                    _ => {
                        let msg = format!("unexpected Content-Type: {}", go_quote(&header));
                        let twerr = bad_route_error(&msg, req.method.as_str(), &path);
                        self.write_error(&mut ctx, twerr)
                    }
                }
            }
            None => {
                let msg = format!("no handler for path {}", go_quote(&path));
                self.write_error(&mut ctx, bad_route_error(&msg, req.method.as_str(), &path))
            }
        }
    }

    // `serveXJSON` and `serveXProtobuf` of each method, the two in one.
    fn serve<Req, Resp>(
        &self,
        ctx: &mut Extensions,
        body: &[u8],
        json: bool,
        (method, req_name, resp_name): (&'static str, &str, &str),
        handler: impl FnOnce(&RoomService, &Extensions, &Req) -> Result<Resp, Error>,
    ) -> Response<Vec<u8>>
    where
        Req: musivum_protocol::Message + Default,
        Resp: musivum_protocol::Message,
    {
        ctx.insert(MethodNameKey(method));
        if let Err(err) = call_request_routed(ctx, self.hooks.as_ref()) {
            return self.write_error(ctx, err);
        }

        let req_content = if json {
            // `json.NewDecoder(req.Body).Decode(&rawReqBody)`: the first JSON value of the body
            let raw_req_body = serde_json::Deserializer::from_slice(body)
                .into_iter::<serde_json::Value>()
                .next();
            match raw_req_body {
                Some(Ok(raw)) => match protojson::unmarshal::<Req>(req_name, &raw) {
                    Ok(r) => r,
                    Err(_) => {
                        return self.handle_request_body_error(
                            ctx,
                            "the json request could not be decoded",
                        );
                    }
                },
                _ => {
                    return self
                        .handle_request_body_error(ctx, "the json request could not be decoded");
                }
            }
        } else {
            match Req::decode(body) {
                Ok(r) => r,
                Err(_) => {
                    return self.write_error(
                        ctx,
                        malformed_request_error("the protobuf request could not be decoded"),
                    );
                }
            }
        };

        // Call service method
        let svc = self.room_service.as_ref();
        let resp_content = {
            let ctx: &Extensions = ctx;
            panic::catch_unwind(AssertUnwindSafe(|| handler(svc, ctx, &req_content)))
        };
        let resp_content = match resp_content {
            Ok(Ok(resp)) => resp,
            // LiveKit's `xtwirp.ServerPassErrorDetails`
            Ok(Err(err)) => return self.write_error(ctx, to_error(&err)),
            Err(p) => return self.ensure_panic_responses(ctx, p),
        };

        call_response_prepared(ctx, self.hooks.as_ref());

        let (resp_bytes, content_type) = if json {
            let opts = MarshalOptions {
                use_proto_names: !self.json_camel_case,
                emit_unpopulated: !self.json_skip_defaults,
            };
            match protojson::marshal(resp_name, &resp_content, opts) {
                Ok(b) => (b, "application/json"),
                Err(err) => {
                    return self
                        .write_error(ctx, wrap_internal(&err, "failed to marshal json response"));
                }
            }
        } else {
            (resp_content.encode_to_vec(), "application/protobuf")
        };

        ctx.insert(StatusCodeKey(StatusCode::OK.as_u16().to_string()));
        let resp = response(StatusCode::OK, content_type, resp_bytes);
        call_response_sent(ctx, self.hooks.as_ref());
        resp
    }

    // writeError writes an HTTP response with a valid Twirp error format, and triggers hooks.
    fn write_error(&self, ctx: &mut Extensions, twerr: TwirpError) -> Response<Vec<u8>> {
        let status_code = server_http_status_from_error_code(twerr.code());
        ctx.insert(StatusCodeKey(status_code.as_u16().to_string()));
        call_error(ctx, self.hooks.as_ref(), &twerr);

        let resp_body = marshal_error_to_json(&twerr);

        // Error responses are always JSON
        let resp = response(status_code, "application/json", resp_body);

        call_response_sent(ctx, self.hooks.as_ref());
        resp
    }

    // handleRequestBodyError is used to handle error when the twirp server cannot read request
    fn handle_request_body_error(&self, ctx: &mut Extensions, msg: &str) -> Response<Vec<u8>> {
        self.write_error(ctx, malformed_request_error(msg))
    }

    // ensurePanicResponses makes sure that rpc methods causing a panic still result in a Twirp
    // Internal error response (status 500), and error hooks are properly called with the panic
    // wrapped as an error.
    fn ensure_panic_responses(
        &self,
        ctx: &mut Extensions,
        p: Box<dyn std::any::Any + Send>,
    ) -> Response<Vec<u8>> {
        let cause = p
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| p.downcast_ref::<String>().cloned())
            .unwrap_or_default();
        errorw!(
            logger::get_logger(),
            "panic serving a request of the room service",
            nil,
            "method" => twirp_method_name(ctx).unwrap_or_default(),
            "panic" => cause
        );
        // the cause is not visible in the response
        self.write_error(ctx, internal_error("Internal service panic"))
    }

    /// `PathPrefix()`: the prefix of the paths of the service's methods, "/twirp" by default:
    /// "/twirp/livekit.RoomService/".
    pub fn path_prefix(&self) -> String {
        base_service_path(&self.path_prefix, "livekit", "RoomService")
    }
}

fn response(status: StatusCode, content_type: &'static str, body: Vec<u8>) -> Response<Vec<u8>> {
    let mut resp = Response::new(Vec::new());
    *resp.status_mut() = status;
    let headers = resp.headers_mut();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    headers.insert(http::header::CONTENT_LENGTH, HeaderValue::from(body.len()));
    *resp.body_mut() = body;
    resp
}

fn call_request_received(
    ctx: &mut Extensions,
    h: Option<&TwirpServerHooks>,
) -> Result<(), TwirpError> {
    match h.and_then(|h| h.request_received.as_ref()) {
        Some(f) => f(ctx),
        None => Ok(()),
    }
}

fn call_request_routed(
    ctx: &mut Extensions,
    h: Option<&TwirpServerHooks>,
) -> Result<(), TwirpError> {
    match h.and_then(|h| h.request_routed.as_ref()) {
        Some(f) => f(ctx),
        None => Ok(()),
    }
}

fn call_response_prepared(ctx: &mut Extensions, h: Option<&TwirpServerHooks>) {
    if let Some(f) = h.and_then(|h| h.response_prepared.as_ref()) {
        f(ctx);
    }
}

fn call_response_sent(ctx: &Extensions, h: Option<&TwirpServerHooks>) {
    if let Some(f) = h.and_then(|h| h.response_sent.as_ref()) {
        f(ctx);
    }
}

fn call_error(ctx: &mut Extensions, h: Option<&TwirpServerHooks>, err: &TwirpError) {
    if let Some(f) = h.and_then(|h| h.error.as_ref()) {
        f(ctx, err);
    }
}

// malformedRequestError is used when the twirp server cannot unmarshal a request
fn malformed_request_error(msg: &str) -> TwirpError {
    new_error(TwirpErrorCode::Malformed, msg)
}

// badRouteError is used when the twirp server cannot route a request
fn bad_route_error(msg: &str, method: &str, url: &str) -> TwirpError {
    new_error(TwirpErrorCode::BadRoute, msg)
        .with_meta("twirp_invalid_route", &format!("{method} {url}"))
}

// wrapInternal wraps an error with a prefix as an Internal error (`twirp.InternalErrorWith`,
// whose `cause` is the Go type of the wrapper the generated code makes).
fn wrap_internal(err: &str, prefix: &str) -> TwirpError {
    internal_error(&format!("{prefix}: {err}")).with_meta("cause", "*livekit.wrappedError")
}

// baseServicePath composes the path prefix for the service (without <Method>).
fn base_service_path(prefix: &str, pkg: &str, service: &str) -> String {
    let full_service_name = if pkg.is_empty() {
        service.to_string()
    } else {
        format!("{pkg}.{service}")
    };
    // `path.Join("/", prefix, fullServiceName) + "/"`
    let mut parts = Vec::new();
    for part in prefix.split('/').chain([full_service_name.as_str()]) {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            part => parts.push(part),
        }
    }
    format!("/{}/", parts.join("/"))
}

// parseTwirpPath extracts path components form a valid Twirp route.
// Expected format: "[<prefix>]/<package>.<Service>/<Method>"
fn parse_twirp_path(path: &str) -> (String, String, String) {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() < 2 {
        return (String::new(), String::new(), String::new());
    }
    let method = parts[parts.len() - 1].to_string();
    let pkg_service = parts[parts.len() - 2].to_string();
    let prefix = parts[..parts.len() - 2].join("/");
    (prefix, pkg_service, method)
}

// marshalErrorToJSON returns JSON from a twirp.Error, that can be used as HTTP error response
// body: `{"code":...,"msg":...,"meta":{...}}` as `encoding/json` writes it, `meta` left out
// when empty.
pub fn marshal_error_to_json(twerr: &TwirpError) -> Vec<u8> {
    // make sure that msg is not too large
    let mut msg = String::new();
    let max = 1_000_000;
    if twerr.msg.len() > max {
        // Go cuts the bytes; `encoding/json` writes each byte of a rune cut in two as U+FFFD
        let mut end = max;
        while !twerr.msg.is_char_boundary(end) {
            end -= 1;
        }
        msg.push_str(&twerr.msg[..end]);
        msg.extend(std::iter::repeat_n('\u{fffd}', max - end));
    } else {
        msg.push_str(&twerr.msg);
    }

    let mut buf = String::from("{\"code\":");
    append_json_string(&mut buf, twerr.code.as_str());
    buf.push_str(",\"msg\":");
    append_json_string(&mut buf, &msg);
    if !twerr.meta.is_empty() {
        buf.push_str(",\"meta\":{");
        for (i, (k, v)) in twerr.meta.iter().enumerate() {
            if i > 0 {
                buf.push(',');
            }
            append_json_string(&mut buf, k);
            buf.push(':');
            append_json_string(&mut buf, v);
        }
        buf.push('}');
    }
    buf.push('}');
    buf.into_bytes()
}

// A string as Go's `encoding/json` writes it, with its escaping of HTML.
fn append_json_string(buf: &mut String, s: &str) {
    buf.push('"');
    for c in s.chars() {
        match c {
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            '\u{8}' => buf.push_str("\\b"),
            '\u{c}' => buf.push_str("\\f"),
            '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => {
                buf.push_str(&format!("\\u{:04x}", c as u32));
            }
            c if c < ' ' => buf.push_str(&format!("\\u{:04x}", c as u32)),
            c => buf.push(c),
        }
    }
    buf.push('"');
}

// Go's `r.Header.Get(key)`: the first value, or nothing.
fn header_get(req: &Parts, key: &str) -> String {
    req.headers
        .get(key)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default()
}

// Go's `req.URL.Path`: the path with its escapes undone. A path with an escape that is not one
// is refused by Go's server before a handler sees it; here it is taken as it is.
fn url_path(req: &Parts) -> String {
    let raw = req.uri.path();
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok());
            match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                Some(b) => {
                    out.push(b);
                    i += 3;
                    continue;
                }
                None => return raw.to_string(),
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

// Go's `%q` of a string (`strconv.Quote`): printable characters as they are, the others
// escaped. What Go takes for printable is approximated by what is neither a control character
// nor a space other than ' '.
fn go_quote(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{b}' => out.push_str("\\v"),
            c if c < ' ' || c == '\u{7f}' => out.push_str(&format!("\\x{:02x}", c as u32)),
            c if c.is_control() || (c.is_whitespace() && c != ' ') => {
                if (c as u32) < 0x10000 {
                    out.push_str(&format!("\\u{:04x}", c as u32));
                } else {
                    out.push_str(&format!("\\U{:08x}", c as u32));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
#[path = "twirp_test.rs"]
mod twirp_test;
