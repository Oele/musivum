// Ported from LiveKit, pkg/rtc/utils.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. `Recover`, which is deferred at the top of a goroutine
// and recovers its panic, cannot be that in Rust: `recover` runs a function and
// `recover_async` a future, both catch its panic, log it as the original does and return the
// result, or as the error what was panicked with (where the original returns that, or nil).
// `IsEOF` compares the kind of a `std::io::Error` with the two kinds that stand for the two
// errors of Go's `io` the original compares with. `ChunkProtoBatch` is generic over prost's
// messages. `LoggerWithPCTarget` logs the target by its name in the protocol, which is what
// its `String()` gives upstream. `net.ParseIP`, `IP.To4` and `IP.IsPrivate` are the functions
// `parse_ip`, `to4` and `is_private` at the end.

use std::any::Any;
use std::fmt;
use std::future;
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::panic::{self, AssertUnwindSafe};
use std::pin::pin;
use std::task::Poll;

use musivum_protocol::Message;

use crate::errorw;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::{self, Logger};
use crate::webrtc::RtpCodecType;

const TRACK_ID_SEPARATOR: &str = "|";

const C_MIN_IP_TRUNCATE_LEN: usize = 8;

pub fn unpack_stream_id(packed: &str) -> (livekit::ParticipantId, livekit::TrackId) {
    let parts: Vec<&str> = packed.split(TRACK_ID_SEPARATOR).collect();
    if parts.len() > 1 {
        return (
            livekit::ParticipantId::from(parts[0]),
            livekit::TrackId::from(&packed[parts[0].len() + 1..]),
        );
    }
    (
        livekit::ParticipantId::from(packed),
        livekit::TrackId::from(""),
    )
}

pub fn pack_stream_id(
    participant_id: &livekit::ParticipantId,
    track_id: &livekit::TrackId,
) -> String {
    participant_id.as_str().to_string() + TRACK_ID_SEPARATOR + track_id.as_str()
}

pub fn pack_sync_stream_id(participant_id: &livekit::ParticipantId, stream: &str) -> String {
    participant_id.as_str().to_string() + TRACK_ID_SEPARATOR + stream
}

pub fn stream_from_track_source(source: livekit::TrackSource) -> &'static str {
    // group camera/mic, screenshare/audio together
    match source {
        livekit::TrackSource::ScreenShare => "screen",
        livekit::TrackSource::ScreenShareAudio => "screen",
        livekit::TrackSource::Camera => "camera",
        livekit::TrackSource::Microphone => "camera",
        _ => "unknown",
    }
}

pub fn pack_data_track_label(
    participant_id: &livekit::ParticipantId,
    track_id: &livekit::TrackId,
    label: &str,
) -> String {
    participant_id.as_str().to_string()
        + TRACK_ID_SEPARATOR
        + track_id.as_str()
        + TRACK_ID_SEPARATOR
        + label
}

pub fn unpack_data_track_label(packed: &str) -> (livekit::ParticipantId, livekit::TrackId, String) {
    let parts: Vec<&str> = packed.split(TRACK_ID_SEPARATOR).collect();
    if parts.len() != 3 {
        return (
            livekit::ParticipantId::from(""),
            livekit::TrackId::from(packed),
            String::new(),
        );
    }
    let participant_id = livekit::ParticipantId::from(parts[0]);
    let track_id = livekit::TrackId::from(parts[1]);
    let label = parts[2].to_string();
    (participant_id, track_id, label)
}

pub fn to_proto_track_kind(kind: RtpCodecType) -> livekit::TrackType {
    match kind {
        RtpCodecType::Video => livekit::TrackType::Video,
        RtpCodecType::Audio => livekit::TrackType::Audio,
        _ => panic!("unsupported track direction"),
    }
}

/// Whether the error is the closed pipe (`io.ErrClosedPipe` upstream) or the end of the input
/// (`io.EOF` upstream).
pub fn is_eof(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::BrokenPipe || err.kind() == io::ErrorKind::UnexpectedEof
}

/// Runs `f` and, if it panics, logs the panic and returns what it panicked with as the error.
///
/// Upstream a goroutine defers `if r := Recover(l); r != nil { os.Exit(1) }`. Here the body
/// of the worker is the function: `if recover(l, || ...).is_err() { std::process::exit(1) }`.
pub fn recover<R>(l: Option<&Logger>, f: impl FnOnce() -> R) -> Result<R, Box<dyn Any + Send>> {
    let r = panic::catch_unwind(AssertUnwindSafe(f));
    if let Err(r) = &r {
        log_recovered_panic(l, r.as_ref());
    }
    r
}

/// [`recover`] for a worker that is a task: polls `f` and, if it panics, logs the panic and
/// returns what it panicked with as the error.
pub async fn recover_async<F: Future>(
    l: Option<&Logger>,
    f: F,
) -> Result<F::Output, Box<dyn Any + Send>> {
    let mut f = pin!(f);
    let r =
        future::poll_fn(
            |cx| match panic::catch_unwind(AssertUnwindSafe(|| f.as_mut().poll(cx))) {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
                Err(r) => Poll::Ready(Err(r)),
            },
        )
        .await;
    if let Err(r) = &r {
        log_recovered_panic(l, r.as_ref());
    }
    r
}

// What `Recover` does with what it recovered. A panic of Rust carries a text (a `&str` or a
// `String`), which is the error that is logged; anything else is the "unknown panic" of the
// original.
fn log_recovered_panic(l: Option<&Logger>, r: &(dyn Any + Send)) {
    let l = match l {
        Some(l) => l.clone(),
        None => logger::get_logger(),
    };
    let (err, r): (&str, &dyn fmt::Debug) = if let Some(e) = r.downcast_ref::<&'static str>() {
        (e, e)
    } else if let Some(e) = r.downcast_ref::<String>() {
        (e.as_str(), e)
    } else {
        ("unknown panic", &r)
    };
    errorw!(l, "recovered panic", err, "panic" => r);
}

// logger helpers
pub fn logger_with_participant(
    l: &Logger,
    identity: &livekit::ParticipantIdentity,
    sid: &livekit::ParticipantId,
    is_remote: bool,
) -> Logger {
    let mut values: Vec<(&str, &dyn fmt::Debug)> = Vec::with_capacity(3);
    if !identity.is_empty() {
        values.push(("participant", identity));
    }
    if !sid.is_empty() {
        values.push(("participantID", sid));
    }
    values.push(("remote", &is_remote));
    // enable sampling per participant
    l.with_values(&values)
}

pub fn logger_with_room(l: &Logger, name: &livekit::RoomName, room_id: &livekit::RoomId) -> Logger {
    let mut values: Vec<(&str, &dyn fmt::Debug)> = Vec::with_capacity(2);
    if !name.is_empty() {
        values.push(("room", name));
    }
    if !room_id.is_empty() {
        values.push(("roomID", room_id));
    }
    // also sample for the room
    l.with_item_sampler().with_values(&values)
}

pub fn logger_with_track(l: &Logger, track_id: &livekit::TrackId, is_relayed: bool) -> Logger {
    // sampling not required because caller already passing in participant's logger
    if !track_id.is_empty() {
        return l.with_values(&[("trackID", track_id), ("relayed", &is_relayed)]);
    }
    l.clone()
}

pub fn logger_with_pc_target(l: &Logger, target: livekit::SignalTarget) -> Logger {
    l.with_values(&[("transport", &target.as_str_name())])
}

pub fn logger_with_codec_mime(l: &Logger, mime_type: mime::MimeType) -> Logger {
    if mime_type != mime::MimeType::Unknown {
        return l.with_values(&[("mime", &mime_type.as_str())]);
    }
    l.clone()
}

pub fn maybe_truncate_ip(addr: &str) -> String {
    let Some(ip_addr) = parse_ip(addr) else {
        return String::new();
    };

    if is_private(&ip_addr) || addr.len() <= C_MIN_IP_TRUNCATE_LEN {
        return addr.to_string();
    }

    addr[..addr.len() - 3].to_string() + "..."
}

pub fn is_ipv6(addr: &str) -> bool {
    let ip_addr = parse_ip(addr);
    ip_addr.is_some_and(|ip_addr| to4(&ip_addr).is_none())
}

pub fn chunk_proto_batch<T: Message>(batch: &[T], target: usize) -> Vec<&[T]> {
    let mut chunks: Vec<&[T]> = Vec::new();
    let mut start = 0;
    let mut size = 0;
    for (i, m) in batch.iter().enumerate() {
        let s = m.encoded_len();
        if size + s > target {
            if start < i {
                chunks.push(&batch[start..i]);
            }
            start = i;
            size = 0;
        }
        size += s;
    }
    if start < batch.len() {
        chunks.push(&batch[start..]);
    }
    chunks
}

pub fn is_red_enabled(ti: &livekit::TrackInfo) -> bool {
    if !ti.codecs.is_empty() && !ti.codecs[0].mime_type.is_empty() {
        return mime::is_mime_type_string_red(&ti.codecs[0].mime_type);
    }

    !ti.disable_red
}

// Go's net.ParseIP(addr): an IPv4 address in dotted decimal or an IPv6 address, without a
// zone. `None` is the nil of the original.
fn parse_ip(addr: &str) -> Option<IpAddr> {
    addr.parse().ok()
}

// Go's ip.To4(): the IPv4 address that an IPv4 address or an IPv4-mapped IPv6 address
// (`::ffff:a.b.c.d`) is.
fn to4(ip: &IpAddr) -> Option<Ipv4Addr> {
    match ip {
        IpAddr::V4(ip4) => Some(*ip4),
        IpAddr::V6(ip6) => ip6.to_ipv4_mapped(),
    }
}

// Go's ip.IsPrivate(): the private IPv4 addresses of RFC 1918 and the unique local IPv6
// addresses of RFC 4193.
fn is_private(ip: &IpAddr) -> bool {
    if let Some(ip4) = to4(ip) {
        return ip4.is_private();
    }
    match ip {
        IpAddr::V4(_) => false,
        IpAddr::V6(ip6) => ip6.is_unique_local(),
    }
}

#[cfg(test)]
#[path = "utils_test.rs"]
mod utils_test;
