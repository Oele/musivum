// Ported from pion/sdp, util.go at v3.0.20.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; only the map of the codecs and the lookup of a payload
// type are here (`GetCodecsForPayloadTypes`, `GetPayloadTypeForCodec` and what they stand on
// are not, nor the connection role, the session id and the lexer); `Codec` is the type of
// the WebRTC library's SDP crate, which has pion's fields, so `appendRTCPFeedback` is a
// function; `get_codec_map` and `get_codec_for_payload_type` are functions that take the
// media descriptions of a session description, which is all the original reads of it, so
// that the codecs of one media description can be asked for without copying it into a
// description of its own; the errors are an enum, and `parse_rtcp_fb` returns
// `ExtractCodecRtcpFb` also where the original returns the error of `strconv.ParseUint`;
// `attribute_string` is `Attribute.String()` of common_description.go, which the library
// writes differently for an attribute with an empty value.

use std::collections::HashMap;
use std::str::FromStr;

use super::{Attribute, Codec, MediaDescription};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("could not extract codec from rtpmap")]
    ExtractCodecRtpmap,
    #[error("could not extract codec from fmtp")]
    ExtractCodecFmtp,
    #[error("could not extract codec from rtcp-fb")]
    ExtractCodecRtcpFb,
    #[error("payload type not found")]
    PayloadTypeNotFound,
}

// Go's strconv.ParseUint(s, 10, bits) with its error dropped: digits only, no sign, and
// nothing for a value that does not fit.
fn parse_uint<T: FromStr>(s: &str) -> Option<T> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

fn attribute_string(a: &Attribute) -> String {
    let mut b = a.key.clone();
    if let Some(value) = &a.value
        && !value.is_empty()
    {
        b.push(':');
        b.push_str(value);
    }

    b
}

fn append_rtcp_feedback(c: &mut Codec, rtcp_feedback: &str) {
    if c.rtcp_feedback.iter().any(|f| f == rtcp_feedback) {
        return;
    }

    c.rtcp_feedback.push(rtcp_feedback.to_string());
}

fn parse_rtpmap(rtpmap: &str) -> Result<Codec, Error> {
    let mut codec = Codec::default();
    let parsing_failed = Error::ExtractCodecRtpmap;

    // a=rtpmap:<payload type> <encoding name>/<clock rate>[/<encoding parameters>]
    let split: Vec<&str> = rtpmap.split(' ').collect();
    if split.len() != 2 {
        return Err(parsing_failed);
    }

    let pt_split: Vec<&str> = split[0].split(':').collect();
    if pt_split.len() != 2 {
        return Err(parsing_failed);
    }

    let Some(pt_int) = parse_uint::<u8>(pt_split[1]) else {
        return Err(parsing_failed);
    };

    codec.payload_type = pt_int;

    let split: Vec<&str> = split[1].split('/').collect();
    codec.name = split[0].to_string();
    let parts = split.len();
    if parts > 1 {
        let Some(rate) = parse_uint::<u32>(split[1]) else {
            return Err(parsing_failed);
        };
        codec.clock_rate = rate;
    }
    if parts > 2 {
        codec.encoding_parameters = split[2].to_string();
    }

    Ok(codec)
}

fn parse_fmtp(fmtp: &str) -> Result<Codec, Error> {
    let mut codec = Codec::default();
    let parsing_failed = Error::ExtractCodecFmtp;

    // a=fmtp:<format> <format specific parameters>
    let Some((format, format_params)) = fmtp.split_once(' ') else {
        return Err(parsing_failed);
    };

    let split: Vec<&str> = format.split(':').collect();
    if split.len() != 2 {
        return Err(parsing_failed);
    }

    let Some(pt_int) = parse_uint::<u8>(split[1]) else {
        return Err(parsing_failed);
    };

    codec.payload_type = pt_int;
    codec.fmtp = format_params.to_string();

    Ok(codec)
}

// The codec and whether it is for all payload types (`rtcp-fb:*`).
fn parse_rtcp_fb(rtcp_fb: &str) -> Result<(Codec, bool), Error> {
    let mut codec = Codec::default();
    let err = Error::ExtractCodecRtcpFb;

    // a=ftcp-fb:<payload type> <RTCP feedback type> [<RTCP feedback parameter>]
    let Some((payload_type, feedback)) = rtcp_fb.split_once(' ') else {
        return Err(err);
    };

    let pt_split: Vec<&str> = payload_type.split(':').collect();
    if pt_split.len() != 2 {
        return Err(err);
    }

    let is_wildcard = pt_split[1] == "*";
    if !is_wildcard {
        let Some(pt_int) = parse_uint::<u8>(pt_split[1]) else {
            return Err(err);
        };

        codec.payload_type = pt_int;
    }

    codec.rtcp_feedback.push(feedback.to_string());

    Ok((codec, is_wildcard))
}

fn merge_codecs(mut codec: Codec, codecs: &mut HashMap<u8, Codec>) {
    let saved_codec = codecs.entry(codec.payload_type).or_default();

    if saved_codec.payload_type == 0 {
        saved_codec.payload_type = codec.payload_type;
    }
    if saved_codec.name.is_empty() {
        saved_codec.name = codec.name;
    }
    if saved_codec.clock_rate == 0 {
        saved_codec.clock_rate = codec.clock_rate;
    }
    if saved_codec.encoding_parameters.is_empty() {
        saved_codec.encoding_parameters = codec.encoding_parameters;
    }
    if saved_codec.fmtp.is_empty() {
        saved_codec.fmtp = codec.fmtp;
    }
    saved_codec.rtcp_feedback.append(&mut codec.rtcp_feedback);
}

// GetCodecMap parses the SessionDescription and returns a map of payload
// type to Codec. This allows callers to build the map once and look up
// multiple payload types without rebuilding it each time.
pub fn get_codec_map(media_descriptions: &[MediaDescription]) -> HashMap<u8, Codec> {
    let static_codec = |payload_type: u8, name: &str| Codec {
        payload_type,
        name: name.to_string(),
        clock_rate: 8000,
        ..Default::default()
    };
    let mut codecs = HashMap::from([
        // static codecs that do not require a rtpmap
        (0, static_codec(0, "PCMU")),
        (8, static_codec(8, "PCMA")),
        (9, static_codec(9, "G722")),
    ]);

    let mut wildcard_rtcp_feedback: Vec<String> = Vec::new();
    for m in media_descriptions {
        for a in &m.attributes {
            let attr = attribute_string(a);
            if attr.starts_with("rtpmap:") {
                if let Ok(codec) = parse_rtpmap(&attr) {
                    merge_codecs(codec, &mut codecs);
                }
            } else if attr.starts_with("fmtp:") {
                if let Ok(codec) = parse_fmtp(&attr) {
                    merge_codecs(codec, &mut codecs);
                }
            } else if attr.starts_with("rtcp-fb:") {
                match parse_rtcp_fb(&attr) {
                    Err(_) => {}
                    Ok((mut codec, true)) => {
                        wildcard_rtcp_feedback.append(&mut codec.rtcp_feedback);
                    }
                    Ok((codec, false)) => merge_codecs(codec, &mut codecs),
                }
            }
        }
    }

    for codec in codecs.values_mut() {
        for new_rtcp_feedback in &wildcard_rtcp_feedback {
            append_rtcp_feedback(codec, new_rtcp_feedback);
        }
    }

    codecs
}

// GetCodecForPayloadType scans the SessionDescription for the given payload type and returns the codec.
pub fn get_codec_for_payload_type(
    media_descriptions: &[MediaDescription],
    payload_type: u8,
) -> Result<Codec, Error> {
    let mut codecs = get_codec_map(media_descriptions);

    match codecs.remove(&payload_type) {
        Some(codec) => Ok(codec),
        None => Err(Error::PayloadTypeNotFound),
    }
}

#[cfg(test)]
#[path = "util_test.rs"]
mod util_test;
