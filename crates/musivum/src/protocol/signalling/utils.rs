// Ported from LiveKit's protocol, signalling/utils.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the descriptions, the candidate and the ICE servers
// are the types of `crate::webrtc::peerconnection`; a message that may be nil is an
// `Option`, a map that is nil an empty one; the type of a description is written as pion
// writes it (`sdp_type_string`: the WebRTC library has another word for one that is not
// set); the JSON of a candidate is written and read as Go's `encoding/json` does it for
// pion's `ICECandidateInit` (`marshal_candidate_init`, `unmarshal_candidate_init`), and what
// is not the same there is said at those two; the error of `from_proto_trickle` is
// serde_json's.

use std::collections::HashMap;
use std::fmt;

use serde::Serialize;
use serde::de::{Deserializer, IgnoredAny, MapAccess, Visitor};

use crate::protocol::livekit;
use crate::webrtc::peerconnection::{IceCandidateInit, IceServer, SdpType, SessionDescription};

pub fn to_proto_session_description(
    sd: SessionDescription,
    id: u32,
    mid_to_track_id: HashMap<String, String>,
) -> Option<livekit::SessionDescription> {
    if sd.sdp.is_empty() {
        return None;
    }

    Some(livekit::SessionDescription {
        r#type: sdp_type_string(sd.r#type).to_string(),
        sdp: sd.sdp,
        id,
        mid_to_track_id,
    })
}

pub fn from_proto_session_description(
    sd: &livekit::SessionDescription,
) -> (SessionDescription, u32, HashMap<String, String>) {
    let sd_type = if sd.r#type == sdp_type_string(SdpType::Offer) {
        SdpType::Offer
    } else if sd.r#type == sdp_type_string(SdpType::Answer) {
        SdpType::Answer
    } else if sd.r#type == sdp_type_string(SdpType::Pranswer) {
        SdpType::Pranswer
    } else if sd.r#type == sdp_type_string(SdpType::Rollback) {
        SdpType::Rollback
    } else {
        SdpType::default()
    };
    (
        SessionDescription {
            r#type: sd_type,
            sdp: sd.sdp.clone(),
        },
        sd.id,
        sd.mid_to_track_id.clone(),
    )
}

pub fn to_proto_trickle(
    candidate_init: &IceCandidateInit,
    target: livekit::SignalTarget,
    r#final: bool,
) -> livekit::TrickleRequest {
    let data = marshal_candidate_init(candidate_init);
    livekit::TrickleRequest {
        candidate_init: data,
        target: target as i32,
        r#final,
    }
}

pub fn from_proto_trickle(
    trickle: &livekit::TrickleRequest,
) -> Result<IceCandidateInit, serde_json::Error> {
    unmarshal_candidate_init(&trickle.candidate_init)
}

pub fn from_proto_ice_servers(iceservers: &[livekit::IceServer]) -> Vec<IceServer> {
    let mut servers = Vec::with_capacity(iceservers.len());
    for server in iceservers {
        servers.push(IceServer {
            urls: server.urls.clone(),
            username: server.username.clone(),
            credential: server.credential.clone(),
        });
    }
    servers
}

// pion's `SDPType.String()`.
fn sdp_type_string(t: SdpType) -> &'static str {
    match t {
        SdpType::Offer => "offer",
        SdpType::Pranswer => "pranswer",
        SdpType::Answer => "answer",
        SdpType::Rollback => "rollback",
        _ => "unknown",
    }
}

// Go's `json.Marshal` of pion's `ICECandidateInit`: the four fields in pion's order, `null`
// for one that is not set, and nothing of what the WebRTC library's type has more. Go writes
// `<`, `>`, `&` and the two separators of lines that JavaScript does not take in a string as
// escapes; serde_json writes them as they are, so they are replaced (they cannot be anywhere
// but in a string).
fn marshal_candidate_init(candidate_init: &IceCandidateInit) -> String {
    #[derive(Serialize)]
    struct IceCandidateInitJson<'a> {
        candidate: &'a str,
        #[serde(rename = "sdpMid")]
        sdp_mid: Option<&'a str>,
        #[serde(rename = "sdpMLineIndex")]
        sdp_mline_index: Option<u16>,
        #[serde(rename = "usernameFragment")]
        username_fragment: Option<&'a str>,
    }

    let data = serde_json::to_string(&IceCandidateInitJson {
        candidate: &candidate_init.candidate,
        sdp_mid: candidate_init.sdp_mid.as_deref(),
        sdp_mline_index: candidate_init.sdp_mline_index,
        username_fragment: candidate_init.username_fragment.as_deref(),
    })
    .unwrap_or_default();

    if !data.contains(['<', '>', '&', '\u{2028}', '\u{2029}']) {
        return data;
    }
    let mut escaped = String::with_capacity(data.len() + 16);
    for c in data.chars() {
        match c {
            '<' => escaped.push_str("\\u003c"),
            '>' => escaped.push_str("\\u003e"),
            '&' => escaped.push_str("\\u0026"),
            '\u{2028}' => escaped.push_str("\\u2028"),
            '\u{2029}' => escaped.push_str("\\u2029"),
            c => escaped.push(c),
        }
    }
    escaped
}

// Go's `json.Unmarshal` into pion's `ICECandidateInit`: a field that is not there is left
// as it is (serde: an error for the candidate), and so is a candidate that is `null`; the
// name of a field matches in any case; of a field that is given more than once, in whatever
// spelling, the last one counts; what is not known is passed over; `null` in place of the
// object gives a candidate with nothing set.
//
// Not the same: Go folds the names by Unicode's rules (a long s is an s), here it is ASCII's
// case; and a string with half of a surrogate pair written as an escape, which Go reads as
// U+FFFD, is an error here.
fn unmarshal_candidate_init(data: &str) -> Result<IceCandidateInit, serde_json::Error> {
    struct IceCandidateInitVisitor;

    impl<'de> Visitor<'de> for IceCandidateInitVisitor {
        type Value = IceCandidateInit;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an ICE candidate init")
        }

        fn visit_unit<E>(self) -> Result<IceCandidateInit, E> {
            Ok(IceCandidateInit::default())
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<IceCandidateInit, A::Error> {
            let mut ci = IceCandidateInit::default();
            while let Some(key) = map.next_key::<String>()? {
                if key.eq_ignore_ascii_case("candidate") {
                    if let Some(candidate) = map.next_value::<Option<String>>()? {
                        ci.candidate = candidate;
                    }
                } else if key.eq_ignore_ascii_case("sdpMid") {
                    ci.sdp_mid = map.next_value()?;
                } else if key.eq_ignore_ascii_case("sdpMLineIndex") {
                    ci.sdp_mline_index = map.next_value()?;
                } else if key.eq_ignore_ascii_case("usernameFragment") {
                    ci.username_fragment = map.next_value()?;
                } else {
                    map.next_value::<IgnoredAny>()?;
                }
            }
            Ok(ci)
        }
    }

    let mut deserializer = serde_json::Deserializer::from_str(data);
    let ci = deserializer.deserialize_any(IceCandidateInitVisitor)?;
    deserializer.end()?;
    Ok(ci)
}

#[cfg(test)]
#[path = "reference_test.rs"]
mod reference_test;
