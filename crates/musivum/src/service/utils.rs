// Ported from LiveKit, pkg/service/utils.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, for any HTTP server: a request is the `http` crate's
// `request::Parts`, the peer's address (Go's `r.RemoteAddr`) is a `RemoteAddr` the server
// puts into its extensions, and the grants are where `auth.rs` puts them. Go's HTTP plumbing
// has no counterpart: `handleError` and `HandleError` return what they would write
// (`ErrorResponse`) and log as the original does; `HandleErrorJson`, `RemoveDoubleSlashes`,
// `WithPathNormalization` and `RequestBodyLimiter` (with `ErrRequestBodyTooLarge`), which are
// middleware and response writers of `net/http`, are not here, nor is the pool of gzip
// readers. The user agent parser of the original (uap-go with uap-core's expressions and a
// rule of its own for OBS Studio) is `useragent.rs`, a recogniser of the browsers the rest of
// the server asks about; see there for what it knows. `AugmentClientInfo` takes the client
// info by reference, which is never nil. `ValidateConnectRequest` takes, in place of the
// router, the node the room is on (on one node: this node), or none where the original's
// router is not a `routing.Router`; it returns the grants it changed as a copy, where the
// original changes the grants in the request's context; its result has public fields, so
// that a server can call it. `form_value` and `Values` are new: what Go's `r.FormValue`
// and `url.ParseQuery` do with a URL's query. The errors of the file are in errors.rs.

use std::collections::HashMap;
use std::fmt;
use std::io::Read;
use std::net::SocketAddr;

use flate2::read::MultiGzDecoder;
use http::request::Parts;
use http::{StatusCode, Uri};

use super::auth::header_value;
use super::useragent::parse_user_agent;
use super::{Error, RoomAllocator, ensure_join_permission, get_grants, get_token_expires_at};
use crate::config::LimitConfig;
use crate::go::time::Time;
use crate::protocol::auth::ClaimGrants;
use crate::protocol::livekit;
use crate::protocol::livekit::client_info::{Capability, Sdk};
use crate::protocol::logger::{self, Logger};
use crate::routing::selector;
use crate::rtc;
use crate::warnw;

// Go's http.DefaultMaxHeaderBytes, the bound on a decompressed payload.
const DEFAULT_MAX_HEADER_BYTES: usize = 1 << 20;

/// Go's `r.RemoteAddr`: the address of the peer a request came from. A server puts it into
/// the request's extensions; without it the address is unknown, as it is to the original
/// when it cannot be split into host and port.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemoteAddr(pub SocketAddr);

pub fn decompress_gzip(compressed: &[u8]) -> Result<Vec<u8>, Error> {
    // Go's gzip.Reader reads one gzip member after another, as MultiGzDecoder does. It reads
    // the header when it is reset, and of an empty input it says EOF, where this decoder
    // says "unexpected end of file": the original's text is kept.
    if compressed.is_empty() {
        return Err(Error::GzipReadFailed("EOF".to_string()));
    }
    let reader = MultiGzDecoder::new(compressed);

    let mut out = Vec::new();
    if let Err(err) = reader
        .take(DEFAULT_MAX_HEADER_BYTES as u64 + 1)
        .read_to_end(&mut out)
    {
        return Err(Error::GzipReadFailed(err.to_string()));
    }
    if out.len() > DEFAULT_MAX_HEADER_BYTES {
        return Err(Error::GzipTooLarge);
    }
    Ok(out)
}

/// What `HandleError` writes: the status, and the error's text as the body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorResponse {
    pub status: StatusCode,
    pub body: String,
}

fn log_error(
    r: &Parts,
    status: StatusCode,
    err: &Error,
    keys_and_values: &[(&str, &dyn fmt::Debug)],
) {
    let status_code = status.as_u16();
    let method = r.method.as_str();
    let path = r.uri.path();
    let mut keys_and_values = keys_and_values.to_vec();
    keys_and_values.push(("status", &status_code));
    keys_and_values.push(("method", &method));
    keys_and_values.push(("path", &path));
    // Go skips the line for a request whose context was cancelled: there is no context here.
    logger::get_logger().warnw("error handling request", Some(err), &keys_and_values);
}

pub fn handle_error(
    r: &Parts,
    status: StatusCode,
    err: &Error,
    keys_and_values: &[(&str, &dyn fmt::Debug)],
) -> ErrorResponse {
    log_error(r, status, err, keys_and_values);
    ErrorResponse {
        status,
        body: err.to_string(),
    }
}

// HandleErrorJson: not ported (it writes to Go's response writer; nothing here answers in
// JSON).

pub fn bool_value(s: &str) -> bool {
    s == "1" || s == "true"
}

// RemoveDoubleSlashes, WithPathNormalization and RequestBodyLimiter: no counterpart (middleware
// of Go's net/http; a server normalises paths and bounds bodies itself).

pub fn is_valid_domain(domain: &str) -> bool {
    // `^(?i)[a-z0-9-]+(\.[a-z0-9-]+)+\.?$`, written out. Go's case folding makes `[a-z]` take
    // the Kelvin sign and the long s as well, which fold to `k` and `s`.
    fn label_char(c: char) -> bool {
        c.is_ascii_alphanumeric() || c == '-' || c == '\u{212A}' || c == '\u{17F}'
    }
    let domain = domain.strip_suffix('.').unwrap_or(domain);
    let labels: Vec<&str> = domain.split('.').collect();
    labels.len() >= 2
        && labels
            .iter()
            .all(|label| !label.is_empty() && label.chars().all(label_char))
}

pub fn get_client_ip(r: &Parts) -> String {
    // CF proxy typically is first thing the user reaches
    let ip = header_value(&r.headers, "CF-Connecting-IP");
    if !ip.is_empty() {
        return ip;
    }
    let ip = header_value(&r.headers, "X-Forwarded-For");
    if !ip.is_empty() {
        return ip;
    }
    let ip = header_value(&r.headers, "X-Real-IP");
    if !ip.is_empty() {
        return ip;
    }
    match r.extensions.get::<RemoteAddr>() {
        Some(addr) => addr.0.ip().to_string(),
        None => String::new(),
    }
}

pub fn set_room_configuration(
    create_request: &mut livekit::CreateRoomRequest,
    conf: Option<&livekit::RoomConfiguration>,
) {
    let Some(conf) = conf else {
        return;
    };
    create_request.agents = conf.agents.clone();
    create_request.egress = conf.egress.clone();
    create_request.empty_timeout = conf.empty_timeout;
    create_request.departure_timeout = conf.departure_timeout;
    create_request.max_participants = conf.max_participants;
    create_request.min_playout_delay = conf.min_playout_delay;
    create_request.max_playout_delay = conf.max_playout_delay;
    create_request.sync_streams = conf.sync_streams;
    create_request.metadata = conf.metadata.clone();
    create_request.tags = conf.tags.clone();
}

pub fn parse_client_info(r: &Parts) -> livekit::ClientInfo {
    let values = Values::parse_query(r.uri.query().unwrap_or(""));
    let mut ci = livekit::ClientInfo::default();
    if let Ok(pv) = values.get("protocol").parse::<i32>() {
        ci.protocol = pv;
    }
    if let Ok(cp) = values.get("client_protocol").parse::<i32>() {
        ci.client_protocol = cp;
    }
    let sdk = match values.get("sdk") {
        "js" => Some(Sdk::Js),
        "ios" | "swift" => Some(Sdk::Swift),
        "android" => Some(Sdk::Android),
        "flutter" => Some(Sdk::Flutter),
        "go" => Some(Sdk::Go),
        "unity" => Some(Sdk::Unity),
        "reactnative" => Some(Sdk::ReactNative),
        "rust" => Some(Sdk::Rust),
        "python" => Some(Sdk::Python),
        "cpp" => Some(Sdk::Cpp),
        "unityweb" => Some(Sdk::UnityWeb),
        "node" => Some(Sdk::Node),
        "esp32" => Some(Sdk::Esp32),
        _ => None,
    };
    if let Some(sdk) = sdk {
        ci.sdk = sdk as i32;
    }

    ci.version = values.get("version").to_string();
    ci.os = values.get("os").to_string();
    ci.os_version = values.get("os_version").to_string();
    ci.browser = values.get("browser").to_string();
    ci.browser_version = values.get("browser_version").to_string();
    ci.device_model = values.get("device_model").to_string();
    ci.network = values.get("network").to_string();

    let cap_str = values.get("capabilities");
    if !cap_str.is_empty() {
        for name in cap_str.split(',') {
            let name = name.trim();
            if name.is_empty() {
                continue;
            }
            if let Some(v) = Capability::from_str_name(name) {
                ci.capabilities.push(v as i32);
            }
        }
    }

    augment_client_info(&mut ci, r);

    ci
}

// userAgentParserCache, createUserAgentParserWithCustomRules and getUserAgentParser: the
// recogniser of useragent.rs needs nothing made in advance; LiveKit's rule for OBS Studio is
// part of it.

pub fn augment_client_info(ci: &mut livekit::ClientInfo, req: &Parts) {
    // get real address (forwarded http header) - check Cloudflare headers first, fall back to X-Forwarded-For
    ci.address = get_client_ip(req);

    // attempt to parse types for SDKs that support browser as a platform
    let sdk = ci.sdk;
    if sdk == Sdk::Js as i32
        || sdk == Sdk::ReactNative as i32
        || sdk == Sdk::Flutter as i32
        || sdk == Sdk::Unity as i32
        || sdk == Sdk::Unknown as i32
    {
        let client = parse_user_agent(&header_value(&req.headers, "User-Agent"));
        if ci.browser.is_empty() {
            ci.browser = client.user_agent.family.clone();
            ci.browser_version = client.user_agent.to_version_string();
        }
        if ci.os.is_empty() {
            ci.os = client.os.family.clone();
            ci.os_version = client.os.to_version_string();
        }
        if ci.device_model.is_empty() {
            let mut model = client.device.family.clone();
            if !model.is_empty() && !client.device.model.is_empty() && model != client.device.model
            {
                model += " ";
                model += &client.device.model;
            }

            ci.device_model = model;
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ValidateConnectRequestParams {
    pub room_name: livekit::RoomName,
    pub publish: String,
    pub metadata: String,
    pub attributes: HashMap<String, String>,
}

#[derive(Clone, Debug, Default)]
pub struct ValidateConnectRequestResult {
    pub room_name: livekit::RoomName,
    pub grants: Option<ClaimGrants>,
    pub token_expires_at: Time,
    pub region: String,
    pub create_room_request: Option<livekit::CreateRoomRequest>,
}

/// The status is that of the result: 200 with `Ok`, the refusal's with `Err`. The result
/// carries what was found before a refusal (the room name), as the original's does.
pub fn validate_connect_request(
    lgr: &Logger,
    r: &Parts,
    limit_config: &LimitConfig,
    params: ValidateConnectRequestParams,
    node: Option<&livekit::Node>,
    room_allocator: &dyn RoomAllocator,
) -> (ValidateConnectRequestResult, StatusCode, Result<(), Error>) {
    let mut res = ValidateConnectRequestResult::default();

    // require a claim
    let mut claims = match get_grants(&r.extensions) {
        Some(claims) if claims.video.is_some() => claims.clone(),
        _ => {
            return (
                res,
                StatusCode::UNAUTHORIZED,
                Err(rtc::Error::PermissionDenied.into()),
            );
        }
    };

    let room_name_in_token = match ensure_join_permission(Some(&claims)) {
        Ok(name) => name,
        Err(err) => return (res, StatusCode::UNAUTHORIZED, Err(err)),
    };

    if claims.identity.is_empty() {
        return (res, StatusCode::BAD_REQUEST, Err(Error::IdentityEmpty));
    }
    if !limit_config.check_participant_identity_length(&claims.identity) {
        return (
            res,
            StatusCode::BAD_REQUEST,
            Err(Error::ParticipantIdentityExceedsLimitsMaxLength(
                limit_config.max_participant_identity_length,
            )),
        );
    }

    if let Some(room_config) = &claims.room_config
        && room_config.check_credentials().is_err()
    {
        warnw!(lgr, "credentials found in token", nil);
        // TODO(dz): in a future version, we'll reject these connections
    }

    res.room_name = params.room_name;
    if !room_name_in_token.is_empty() {
        res.room_name = room_name_in_token;
    }
    if res.room_name.is_empty() {
        return (res, StatusCode::BAD_REQUEST, Err(Error::NoRoomName));
    }
    if !limit_config.check_room_name_length(res.room_name.as_str()) {
        let max_length = limit_config.max_room_name_length;
        return (
            res,
            StatusCode::BAD_REQUEST,
            Err(Error::RoomNameExceedsLimitsMaxLength(max_length)),
        );
    }

    // this is new connection for existing participant -  with publish only permissions
    if !params.publish.is_empty()
        && let Some(video) = claims.video.as_mut()
    {
        // Make sure grant has GetCanPublish set,
        if !video.get_can_publish() {
            return (
                res,
                StatusCode::UNAUTHORIZED,
                Err(rtc::Error::PermissionDenied.into()),
            );
        }
        // Make sure by default subscribe is off
        video.set_can_subscribe(false);
        claims.identity += "#";
        claims.identity += &params.publish;
    }

    // room allocator validations
    if let Err(err) = room_allocator.validate_create_room(Some(&claims), &res.room_name) {
        if err == Error::RoomNotFound {
            return (res, StatusCode::NOT_FOUND, Err(err));
        } else {
            return (res, StatusCode::INTERNAL_SERVER_ERROR, Err(err));
        }
    }

    // `if router, ok := router.(routing.Router)`: the region of the router's node, and the
    // limits of the node the room is on, which for one node are this node's
    if let Some(found_node) = node {
        res.region = found_node.region.clone();
        if selector::limits_reached(limit_config, found_node.stats.as_ref()) {
            return (
                res,
                StatusCode::SERVICE_UNAVAILABLE,
                Err(rtc::Error::LimitExceeded.into()),
            );
        }
    }

    let mut create_request = livekit::CreateRoomRequest {
        name: res.room_name.to_string(),
        room_preset: claims.room_preset.clone(),
        ..Default::default()
    };
    set_room_configuration(&mut create_request, claims.get_room_configuration());
    res.create_room_request = Some(create_request);

    if !params.metadata.is_empty() {
        // Make sure grant has GetCanUpdateOwnMetadata set
        if !claims
            .video
            .as_ref()
            .is_some_and(|v| v.get_can_update_own_metadata())
        {
            return (
                res,
                StatusCode::UNAUTHORIZED,
                Err(rtc::Error::PermissionDenied.into()),
            );
        }
        claims.metadata = params.metadata;
    }

    // Add extra attributes to the participant
    if !params.attributes.is_empty() {
        // Make sure grant has GetCanUpdateOwnMetadata set
        if !claims
            .video
            .as_ref()
            .is_some_and(|v| v.get_can_update_own_metadata())
        {
            return (
                res,
                StatusCode::UNAUTHORIZED,
                Err(rtc::Error::PermissionDenied.into()),
            );
        }
        for (k, v) in params.attributes {
            if v.is_empty() {
                continue; // do not allow deleting existing attributes
            }
            claims.attributes.insert(k, v);
        }
    }

    res.grants = Some(claims);
    res.token_expires_at = get_token_expires_at(&r.extensions);
    (res, StatusCode::OK, Ok(()))
}

pub fn is_rtc_path(path: &str) -> bool {
    path == "/rtc" || path == "/rtc/v1"
}

pub fn is_rtc_validate_path(path: &str) -> bool {
    path == "/rtc/validate" || path == "/rtc/v1/validate"
}

pub fn is_agent_worker_path(path: &str) -> bool {
    path == "/agent"
}

pub fn is_agent_path(path: &str) -> bool {
    path.starts_with("/agent")
}

/// Go's `r.FormValue(key)` for a request without a form in its body (a WebSocket upgrade,
/// a GET): the first value of `key` in the URL's query, or the empty string.
pub fn form_value(r: &Parts, key: &str) -> String {
    query_value(&r.uri, key)
}

/// Go's `u.Query().Get(key)`.
pub fn query_value(uri: &Uri, key: &str) -> String {
    Values::parse_query(uri.query().unwrap_or(""))
        .get(key)
        .to_string()
}

/// Go's `url.Values` as `url.ParseQuery` makes them: the pairs of a query, in order, with
/// what Go leaves out left out (a pair with a semicolon, a pair whose escapes are wrong).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Values(pub Vec<(String, String)>);

impl Values {
    pub fn parse_query(query: &str) -> Values {
        let mut pairs = Vec::new();
        for pair in query.split('&') {
            if pair.contains(';') {
                // "invalid semicolon separator in query"
                continue;
            }
            if pair.is_empty() {
                continue;
            }
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let (Some(key), Some(value)) = (query_unescape(key), query_unescape(value)) else {
                continue;
            };
            pairs.push((key, value));
        }
        Values(pairs)
    }

    /// Go's `Values.Get`: the first value of the key.
    pub fn get(&self, key: &str) -> &str {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .unwrap_or("")
    }
}

// Go's url.QueryUnescape: `+` is a space, `%` takes two hexadecimal digits; None where Go
// finds an invalid escape.
fn query_unescape(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = bytes.get(i + 1..i + 3)?;
                let hi = (hex[0] as char).to_digit(16)?;
                let lo = (hex[1] as char).to_digit(16)?;
                out.push((hi * 16 + lo) as u8);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    Some(String::from_utf8_lossy(&out).into_owned())
}

#[cfg(test)]
#[path = "utils_test.rs"]
mod utils_test;
