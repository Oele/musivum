//! The JSON form of the room service's messages as its Twirp server reads and writes them
//! (twirp.rs). Written new for musivum.
//!
//! The generated Twirp server of the original reads a request with Go's `protojson` and
//! `DiscardUnknown` (fields it does not know are passed over) and writes a response with
//! `UseProtoNames` (the names of the `.proto` file, `empty_timeout`, not `emptyTimeout`) and
//! `EmitUnpopulated` (every field, also those with their default value). The serde code that
//! pbjson generates for the protocol's types does neither, and is used elsewhere as it is, so
//! the messages are read and written here by their descriptors (`prost-reflect`, from
//! `musivum_protocol::FILE_DESCRIPTOR_SET`), following what Go's protobuf module does (read at
//! google.golang.org/protobuf v1.36.12, `encoding/protojson` and `internal/encoding/json`;
//! nothing copied):
//!
//! - Writing: fields in the order of the `.proto` file; with `EmitUnpopulated` an unset field
//!   of a message type is `null`, an unset field of a oneof (also of the oneof an `optional`
//!   field is in) is left out, any other field has its default value; 64-bit integers are
//!   strings, enums their names (their numbers when unknown), bytes base64 with padding,
//!   floats as Go formats them (`1e+21`, `1e-7`, `"NaN"`), map entries sorted by key; strings
//!   escaped as Go's encoder escapes them (no HTML escaping). Go's encoder puts a space after
//!   a comma or not, deliberately at random per binary (`detrand`); here there is none.
//! - Reading: a field by its JSON name or its name in the `.proto` file; unknown fields and
//!   unknown enum names passed over; `null` leaves a field unset; numbers also as strings,
//!   64-bit integers as numbers; bytes in standard or URL base64, with or without padding; a
//!   field given twice (by both its names) or two fields of one oneof are an error. A key given
//!   twice in the same spelling, which Go refuses, is not seen here (the JSON is read into
//!   `serde_json::Value`, which keeps the last).
//! - The well-known types of `google.protobuf` have JSON forms of their own; none is reached
//!   from the room service's messages (a test checks it), and meeting one is an error.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::OnceLock;

use base64::Engine as _;
use base64::alphabet;
use base64::engine::general_purpose::STANDARD;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use musivum_protocol::Message;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor,
    ReflectMessage as _, Value,
};

/// What `protojson.MarshalOptions` the server writes with.
#[derive(Clone, Copy, Debug)]
pub(super) struct MarshalOptions {
    pub(super) use_proto_names: bool,
    pub(super) emit_unpopulated: bool,
}

fn pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| {
        DescriptorPool::decode(musivum_protocol::FILE_DESCRIPTOR_SET)
            .expect("the protocol's descriptors are those its types are generated from")
    })
}

pub(super) fn message_descriptor(full_name: &str) -> Result<MessageDescriptor, String> {
    pool()
        .get_message_by_name(full_name)
        .ok_or_else(|| format!("no message {full_name}"))
}

/// `protojson.MarshalOptions{...}.Marshal(msg)` of the message named `full_name`.
pub(super) fn marshal<M: Message>(
    full_name: &str,
    msg: &M,
    opts: MarshalOptions,
) -> Result<Vec<u8>, String> {
    let desc = message_descriptor(full_name)?;
    let msg = DynamicMessage::decode(desc, msg.encode_to_vec().as_slice())
        .map_err(|err| err.to_string())?;
    let mut e = Encoder {
        out: String::new(),
        opts,
    };
    e.write_message(&msg)?;
    Ok(e.out.into_bytes())
}

/// `protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(json, msg)` of the message named
/// `full_name`.
pub(super) fn unmarshal<M: Message + Default>(
    full_name: &str,
    json: &serde_json::Value,
) -> Result<M, String> {
    let desc = message_descriptor(full_name)?;
    let msg = read_message(&desc, json)?;
    M::decode(msg.encode_to_vec().as_slice()).map_err(|err| err.to_string())
}

fn is_well_known_type(desc: &MessageDescriptor) -> bool {
    desc.package_name() == "google.protobuf"
}

// The fields of a message in the order of the `.proto` file (Go's `order.IndexNameFieldOrder`).
fn fields_in_index_order(desc: &MessageDescriptor) -> Vec<FieldDescriptor> {
    desc.descriptor_proto()
        .field
        .iter()
        .filter_map(|f| desc.get_field(f.number() as u32))
        .collect()
}

// ------------------------------------------------
// writing

struct Encoder {
    out: String,
    opts: MarshalOptions,
}

impl Encoder {
    fn write_message(&mut self, msg: &DynamicMessage) -> Result<(), String> {
        let desc = msg.descriptor();
        if is_well_known_type(&desc) {
            return Err(format!("no JSON form for {}", desc.full_name()));
        }

        self.out.push('{');
        let mut first = true;
        for fd in fields_in_index_order(&desc) {
            let value: Option<Cow<'_, Value>> = if msg.has_field(&fd) {
                Some(msg.get_field(&fd))
            } else if self.opts.emit_unpopulated && fd.containing_oneof().is_none() {
                if fd.supports_presence() {
                    None // null
                } else {
                    Some(msg.get_field(&fd))
                }
            } else {
                continue;
            };

            if !first {
                self.out.push(',');
            }
            first = false;
            let name = if self.opts.use_proto_names {
                fd.name()
            } else {
                fd.json_name()
            };
            append_string(&mut self.out, name);
            self.out.push(':');
            match value {
                None => self.out.push_str("null"),
                Some(value) => self.write_value(&value, &fd)?,
            }
        }
        self.out.push('}');
        Ok(())
    }

    fn write_value(&mut self, value: &Value, fd: &FieldDescriptor) -> Result<(), String> {
        let kind = fd.kind();
        match value {
            Value::List(items) => {
                self.out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        self.out.push(',');
                    }
                    self.write_singular(item, &kind)?;
                }
                self.out.push(']');
            }
            Value::Map(entries) => {
                let value_kind = match &kind {
                    Kind::Message(entry) => entry.map_entry_value_field().kind(),
                    _ => return Err(format!("map field {} of no entry", fd.full_name())),
                };
                let mut entries: Vec<(&MapKey, &Value)> = entries.iter().collect();
                entries.sort_by(|a, b| compare_map_keys(a.0, b.0));
                self.out.push('{');
                for (i, (key, value)) in entries.into_iter().enumerate() {
                    if i > 0 {
                        self.out.push(',');
                    }
                    append_string(&mut self.out, &map_key_string(key));
                    self.out.push(':');
                    self.write_singular(value, &value_kind)?;
                }
                self.out.push('}');
            }
            value => self.write_singular(value, &kind)?,
        }
        Ok(())
    }

    fn write_singular(&mut self, value: &Value, kind: &Kind) -> Result<(), String> {
        match value {
            Value::Bool(b) => self.out.push_str(if *b { "true" } else { "false" }),
            Value::String(s) => append_string(&mut self.out, s),
            Value::I32(n) => {
                let _ = write!(self.out, "{n}");
            }
            Value::U32(n) => {
                let _ = write!(self.out, "{n}");
            }
            Value::I64(n) => append_string(&mut self.out, &n.to_string()),
            Value::U64(n) => append_string(&mut self.out, &n.to_string()),
            Value::F32(f) => append_float(&mut self.out, *f as f64, 32),
            Value::F64(f) => append_float(&mut self.out, *f, 64),
            Value::Bytes(b) => append_string(&mut self.out, &STANDARD.encode(b)),
            Value::EnumNumber(n) => match kind {
                Kind::Enum(ed) if ed.full_name() == "google.protobuf.NullValue" => {
                    self.out.push_str("null")
                }
                Kind::Enum(ed) => match ed.get_value(*n) {
                    Some(v) => append_string(&mut self.out, v.name()),
                    None => {
                        let _ = write!(self.out, "{n}");
                    }
                },
                _ => return Err("an enum value of a field that is no enum".into()),
            },
            Value::Message(msg) => self.write_message(msg)?,
            Value::List(_) | Value::Map(_) => {
                return Err("a list or a map where one value belongs".into());
            }
        }
        Ok(())
    }
}

// Go's `order.GenericKeyOrder`
fn compare_map_keys(a: &MapKey, b: &MapKey) -> Ordering {
    match (a, b) {
        (MapKey::Bool(a), MapKey::Bool(b)) => a.cmp(b),
        (MapKey::I32(a), MapKey::I32(b)) => a.cmp(b),
        (MapKey::I64(a), MapKey::I64(b)) => a.cmp(b),
        (MapKey::U32(a), MapKey::U32(b)) => a.cmp(b),
        (MapKey::U64(a), MapKey::U64(b)) => a.cmp(b),
        (MapKey::String(a), MapKey::String(b)) => a.as_bytes().cmp(b.as_bytes()),
        _ => Ordering::Equal,
    }
}

fn map_key_string(key: &MapKey) -> String {
    match key {
        MapKey::Bool(b) => b.to_string(),
        MapKey::I32(n) => n.to_string(),
        MapKey::I64(n) => n.to_string(),
        MapKey::U32(n) => n.to_string(),
        MapKey::U64(n) => n.to_string(),
        MapKey::String(s) => s.clone(),
    }
}

// A string as Go's `internal/encoding/json` writes it.
fn append_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < ' ' => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

// A float as Go's `internal/encoding/json` writes it: `strconv.AppendFloat` with the shortest
// digits, in exponent form below 1e-6 and from 1e21, with a one-digit negative exponent
// without its leading zero.
fn append_float(out: &mut String, n: f64, bit_size: u32) {
    if n.is_nan() {
        out.push_str("\"NaN\"");
        return;
    }
    if n == f64::INFINITY {
        out.push_str("\"Infinity\"");
        return;
    }
    if n == f64::NEG_INFINITY {
        out.push_str("\"-Infinity\"");
        return;
    }

    let abs = n.abs();
    let exponent_form = abs != 0.0
        && ((bit_size == 64 && (abs < 1e-6 || abs >= 1e21))
            || (bit_size == 32 && ((abs as f32) < 1e-6 || (abs as f32) >= 1e21)));
    let formatted = match (bit_size, exponent_form) {
        (32, false) => format!("{}", n as f32),
        (32, true) => format!("{:e}", n as f32),
        (_, false) => format!("{n}"),
        (_, true) => format!("{n:e}"),
    };
    if !exponent_form {
        out.push_str(&formatted);
        return;
    }
    // Rust writes `1e21` and `1.5e-7`, Go `1e+21` and `1.5e-7` (after its fix of `e-07`)
    match formatted.split_once('e') {
        Some((mantissa, exp)) if !exp.starts_with('-') => {
            let _ = write!(out, "{mantissa}e+{exp}");
        }
        _ => out.push_str(&formatted),
    }
}

// ------------------------------------------------
// reading

fn read_message(desc: &MessageDescriptor, v: &serde_json::Value) -> Result<DynamicMessage, String> {
    if is_well_known_type(desc) {
        return Err(format!("no JSON form for {}", desc.full_name()));
    }
    let Some(obj) = v.as_object() else {
        return Err(format!("unexpected token {v}"));
    };

    let mut msg = DynamicMessage::new(desc.clone());
    let mut seen_nums = HashSet::new();
    let mut seen_oneofs = HashSet::new();
    for (name, val) in obj {
        let fd = desc
            .get_field_by_json_name(name)
            .or_else(|| desc.get_field_by_name(name));
        // DiscardUnknown
        let Some(fd) = fd else {
            continue;
        };

        if !seen_nums.insert(fd.number()) {
            return Err(format!("duplicate field {name:?}"));
        }

        if val.is_null() && !is_null_value(&fd.kind()) {
            continue;
        }

        let kind = fd.kind();
        if fd.is_list() {
            let Some(items) = val.as_array() else {
                return Err(format!("unexpected token {val}"));
            };
            let mut list = Vec::with_capacity(items.len());
            for item in items {
                match &kind {
                    Kind::Message(md) => list.push(Value::Message(read_message(md, item)?)),
                    kind => {
                        if let Some(v) = read_scalar(kind, item)? {
                            list.push(v);
                        }
                    }
                }
            }
            msg.set_field(&fd, Value::List(list));
        } else if fd.is_map() {
            let (Kind::Message(entry), Some(entries)) = (&kind, val.as_object()) else {
                return Err(format!("unexpected token {val}"));
            };
            let key_kind = entry.map_entry_key_field().kind();
            let value_kind = entry.map_entry_value_field().kind();
            let mut map = HashMap::with_capacity(entries.len());
            for (key, value) in entries {
                let key = read_map_key(&key_kind, key)?;
                if map.contains_key(&key) {
                    return Err(format!("duplicate map key {key:?}"));
                }
                let value = match &value_kind {
                    Kind::Message(md) => Some(Value::Message(read_message(md, value)?)),
                    kind => read_scalar(kind, value)?,
                };
                if let Some(value) = value {
                    map.insert(key, value);
                }
            }
            msg.set_field(&fd, Value::Map(map));
        } else {
            if let Some(od) = fd.containing_oneof()
                && !seen_oneofs.insert(od.name().to_string())
            {
                return Err(format!(
                    "error parsing {name:?}, oneof {} is already set",
                    od.full_name()
                ));
            }

            let value = match &kind {
                Kind::Message(md) => Some(Value::Message(read_message(md, val)?)),
                kind => read_scalar(kind, val)?,
            };
            if let Some(value) = value {
                msg.set_field(&fd, value);
            }
        }
    }
    Ok(msg)
}

fn is_null_value(kind: &Kind) -> bool {
    matches!(kind, Kind::Enum(ed) if ed.full_name() == "google.protobuf.NullValue")
}

// `None` where Go's decoder gives no value without an error: an unknown enum name, which
// `DiscardUnknown` passes over.
fn read_scalar(kind: &Kind, v: &serde_json::Value) -> Result<Option<Value>, String> {
    let value = match kind {
        Kind::Bool => v.as_bool().map(Value::Bool),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => read_int(v)
            .and_then(|n| i32::try_from(n).ok())
            .map(Value::I32),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => read_int(v).map(Value::I64),
        Kind::Uint32 | Kind::Fixed32 => read_uint(v)
            .and_then(|n| u32::try_from(n).ok())
            .map(Value::U32),
        Kind::Uint64 | Kind::Fixed64 => read_uint(v).map(Value::U64),
        Kind::Float => read_float(v, 32).map(|f| Value::F32(f as f32)),
        Kind::Double => read_float(v, 64).map(Value::F64),
        Kind::String => v.as_str().map(|s| Value::String(s.to_string())),
        Kind::Bytes => v
            .as_str()
            .and_then(read_bytes)
            .map(|b| Value::Bytes(b.into())),
        Kind::Enum(ed) => match v {
            serde_json::Value::String(s) => {
                return Ok(ed
                    .get_value_by_name(s)
                    .map(|value| Value::EnumNumber(value.number())));
            }
            serde_json::Value::Number(n) => number_as_int(n)
                .and_then(|n| i32::try_from(n).ok())
                .map(Value::EnumNumber),
            serde_json::Value::Null if is_null_value(kind) => Some(Value::EnumNumber(0)),
            _ => None,
        },
        Kind::Message(_) => None,
    };
    match value {
        Some(value) => Ok(Some(value)),
        None => Err(format!("invalid value for {kind:?} field: {v}")),
    }
}

// A number, or a string that holds one and nothing else (not even spaces around it).
fn number_of(v: &serde_json::Value) -> Option<serde_json::Number> {
    match v {
        serde_json::Value::Number(n) => Some(n.clone()),
        serde_json::Value::String(s) => {
            if s.trim() != s {
                return None;
            }
            serde_json::from_str::<serde_json::Number>(s).ok()
        }
        _ => None,
    }
}

// An integer in any form a JSON number can take (`1`, `1.0`, `1e2`), as Go's decoder reads it.
fn number_as_int(n: &serde_json::Number) -> Option<i64> {
    if let Some(i) = n.as_i64() {
        return Some(i);
    }
    if n.is_u64() {
        return None;
    }
    let f = n.as_f64()?;
    (f.fract() == 0.0 && f >= i64::MIN as f64 && f < i64::MAX as f64).then_some(f as i64)
}

fn read_int(v: &serde_json::Value) -> Option<i64> {
    number_as_int(&number_of(v)?)
}

fn read_uint(v: &serde_json::Value) -> Option<u64> {
    let n = number_of(v)?;
    if let Some(u) = n.as_u64() {
        return Some(u);
    }
    if n.is_i64() {
        return None;
    }
    let f = n.as_f64()?;
    (f.fract() == 0.0 && f >= 0.0 && f < u64::MAX as f64).then_some(f as u64)
}

fn read_float(v: &serde_json::Value, bit_size: u32) -> Option<f64> {
    if let serde_json::Value::String(s) = v {
        match s.as_str() {
            "NaN" => return Some(f64::NAN),
            "Infinity" => return Some(f64::INFINITY),
            "-Infinity" => return Some(f64::NEG_INFINITY),
            _ => {}
        }
    }
    let f = number_of(v)?.as_f64()?;
    // Go's `ParseFloat(s, 32)` refuses what is out of the range of a float32
    if bit_size == 32 && (f as f32).is_infinite() {
        return None;
    }
    Some(f)
}

// Standard base64, or the URL alphabet where `-` or `_` appear; padded when the length is a
// multiple of four. Go's decoder (not strict) accepts bits after the last full byte.
fn read_bytes(s: &str) -> Option<Vec<u8>> {
    let alphabet = if s.contains(['-', '_']) {
        &alphabet::URL_SAFE
    } else {
        &alphabet::STANDARD
    };
    let padding = if s.len().is_multiple_of(4) {
        DecodePaddingMode::RequireCanonical
    } else {
        DecodePaddingMode::RequireNone
    };
    let engine = GeneralPurpose::new(
        alphabet,
        GeneralPurposeConfig::new()
            .with_decode_allow_trailing_bits(true)
            .with_decode_padding_mode(padding),
    );
    engine.decode(s).ok()
}

fn read_map_key(kind: &Kind, key: &str) -> Result<MapKey, String> {
    let k = match kind {
        Kind::String => Some(MapKey::String(key.to_string())),
        Kind::Bool => match key {
            "true" => Some(MapKey::Bool(true)),
            "false" => Some(MapKey::Bool(false)),
            _ => None,
        },
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => key.parse().ok().map(MapKey::I32),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => key.parse().ok().map(MapKey::I64),
        Kind::Uint32 | Kind::Fixed32 => key.parse().ok().map(MapKey::U32),
        Kind::Uint64 | Kind::Fixed64 => key.parse().ok().map(MapKey::U64),
        _ => None,
    };
    k.ok_or_else(|| format!("invalid value for {kind:?} key: {key:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::livekit;

    // The room service's messages, those of the requests and of the responses.
    const ROOM_SERVICE_MESSAGES: &[&str] = &[
        "livekit.CreateRoomRequest",
        "livekit.Room",
        "livekit.ListRoomsRequest",
        "livekit.ListRoomsResponse",
        "livekit.DeleteRoomRequest",
        "livekit.DeleteRoomResponse",
        "livekit.ListParticipantsRequest",
        "livekit.ListParticipantsResponse",
        "livekit.RoomParticipantIdentity",
        "livekit.ParticipantInfo",
        "livekit.RemoveParticipantResponse",
        "livekit.MuteRoomTrackRequest",
        "livekit.MuteRoomTrackResponse",
        "livekit.UpdateParticipantRequest",
        "livekit.UpdateSubscriptionsRequest",
        "livekit.UpdateSubscriptionsResponse",
        "livekit.SendDataRequest",
        "livekit.SendDataResponse",
        "livekit.UpdateRoomMetadataRequest",
        "livekit.ForwardParticipantRequest",
        "livekit.ForwardParticipantResponse",
        "livekit.MoveParticipantRequest",
        "livekit.MoveParticipantResponse",
        "livekit.PerformRpcRequest",
        "livekit.PerformRpcResponse",
    ];

    #[test]
    fn no_well_known_type_is_reached_from_the_room_service() {
        let mut seen = HashSet::new();
        let mut todo: Vec<MessageDescriptor> = ROOM_SERVICE_MESSAGES
            .iter()
            .map(|name| message_descriptor(name).unwrap())
            .collect();
        while let Some(desc) = todo.pop() {
            if !seen.insert(desc.full_name().to_string()) {
                continue;
            }
            assert!(
                !is_well_known_type(&desc),
                "{} is reached",
                desc.full_name()
            );
            for fd in desc.fields() {
                if let Kind::Message(md) = fd.kind() {
                    todo.push(md);
                }
            }
        }
        assert!(seen.len() > ROOM_SERVICE_MESSAGES.len());
    }

    #[test]
    fn floats_are_written_as_go_writes_them() {
        let f = |n: f64, bits: u32| {
            let mut out = String::new();
            append_float(&mut out, n, bits);
            out
        };
        assert_eq!("1e+21", f(1e21, 64));
        assert_eq!("1e-7", f(1e-7, 64));
        assert_eq!("1.5e-10", f(1.5e-10, 64));
        assert_eq!("123456.789", f(123456.789, 64));
        assert_eq!("100000000000000000000", f(1e20, 64));
        assert_eq!("0.1", f(0.1f32 as f64, 32));
        assert_eq!("16777216", f(16777217.0, 32));
        assert_eq!("1e+21", f(1e21, 32));
        assert_eq!("-0", f(-0.0, 64));
        assert_eq!("\"NaN\"", f(f64::NAN, 64));
        assert_eq!("\"-Infinity\"", f(f64::NEG_INFINITY, 32));
    }

    #[test]
    fn requests_are_read_as_protojson_reads_them() {
        let read = |json: &str| {
            unmarshal::<livekit::SendDataRequest>(
                "livekit.SendDataRequest",
                &serde_json::from_str(json).unwrap(),
            )
        };
        // URL base64 without padding, an enum by number, a field given `null`
        let req = read(r#"{"data":"-_8","kind":1,"room":null,"destinationSids":["a"]}"#).unwrap();
        assert_eq!(vec![0xfb, 0xff], req.data);
        assert_eq!(livekit::data_packet::Kind::Lossy as i32, req.kind);
        assert_eq!("", req.room);
        assert_eq!(vec!["a".to_string()], req.destination_sids);
        // an unknown enum name is passed over, an unknown field too
        let req = read(r#"{"kind":"NO_SUCH_KIND","whatever":[{}]}"#).unwrap();
        assert_eq!(livekit::data_packet::Kind::Reliable as i32, req.kind);
        // a field by both its names, a value of the wrong type
        assert!(read(r#"{"destination_sids":[],"destinationSids":[]}"#).is_err());
        assert!(read(r#"{"data":5}"#).is_err());

        let read = |json: &str| {
            unmarshal::<livekit::PerformRpcRequest>(
                "livekit.PerformRpcRequest",
                &serde_json::from_str(json).unwrap(),
            )
        };
        // numbers as strings and in other forms, but not with spaces around them
        assert_eq!(
            100,
            read(r#"{"responseTimeoutMs":"1e2"}"#)
                .unwrap()
                .response_timeout_ms
        );
        assert_eq!(
            7,
            read(r#"{"response_timeout_ms":7.0}"#)
                .unwrap()
                .response_timeout_ms
        );
        assert!(read(r#"{"responseTimeoutMs":" 7"}"#).is_err());
        assert!(read(r#"{"responseTimeoutMs":-1}"#).is_err());
        assert!(read(r#"{"responseTimeoutMs":1.5}"#).is_err());
    }
}
