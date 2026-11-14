//! Reading and writing JSON the way the Go code of this package does. Written new for
//! musivum.
//!
//! The original reads the claims of a token with Go's `encoding/json`, and the room
//! configuration in them with protojson. What LiveKit takes for a token has to be taken here
//! too, so where those two readers differ from serde, a value read through [`from_slice`]
//! or [`from_value`] is read by their rules:
//!
//! - A field that is `null` is left as it is. (serde: an error, unless it is an `Option`.)
//! - The name of a field of a Go struct matches in any case (`"Video"` is `video`,
//!   `"RoomJoin"` is `roomJoin`), unless a field has exactly that name. Tokens made from
//!   structs whose fields were not given JSON names rely on it.
//! - What a message of the protocol does not know is left out: protojson with
//!   `DiscardUnknown`, which keeps the token of a newer client readable. The names of its
//!   fields match exactly, as their JSON name or as their name in the `.proto` file. A
//!   field that is `null` is not set; a message that is `null`, in a list or a map, is an
//!   error.
//! - A number without a fraction is an integer to a message of the protocol however it is
//!   written (`5.0`, `1e1`).
//! - In a string, a byte that is not UTF-8 and an escape that is half of a surrogate pair
//!   (`"\ud83d"`, what JavaScript writes for a string that was cut in the middle of a
//!   character) are U+FFFD. (serde: an error.)
//!
//! Not the same as in Go, none of which a token of LiveKit's SDKs runs into:
//!
//! - When a field is given twice, Go takes each in turn (the last wins, and two objects are
//!   merged); here the last wins. When it is given in two spellings, the exact one wins
//!   here, the last one in Go. In a message of the protocol protojson refuses a field that
//!   is given twice; here the last wins.
//! - A value of an enumeration in a message of the protocol that is not known is an error
//!   here. protojson leaves out a name it does not know and keeps a number it does not
//!   know.
//! - An integer of a message of the protocol that is written in quotes with a fraction or
//!   an exponent (`"5.0"`) is an error here; protojson takes it.
//! - Text that is not UTF-8 and half surrogate pairs are U+FFFD in a message of the protocol
//!   too, where protojson refuses them.
//! - What is not known is still read as JSON by serde_json, which refuses a number that is
//!   more than a float holds (`1e999`) and lists or objects nested more than 127 deep. Go
//!   does not look at what it does not know.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

use serde::de::value::{MapDeserializer, SeqDeserializer};
use serde::de::{
    DeserializeOwned, Deserializer, Error as _, IntoDeserializer, Unexpected, Visitor,
};
use serde::{Deserialize, Serializer};
use serde_json::{Error, Value};

/// Go's `json.Unmarshal(data, &v)`.
pub(super) fn from_slice<T: DeserializeOwned>(data: &[u8]) -> Result<T, Error> {
    from_value(serde_json::from_str(&go_text(data))?)
}

// A JSON document as text in which serde_json reads the strings that Go's decoder reads:
// a byte that is not UTF-8 is U+FFFD (one for each such byte, as Go has it), and so is an
// escape that is half of a surrogate pair. Outside a string either is not JSON, before and
// after.
fn go_text(data: &[u8]) -> Cow<'_, str> {
    let mut text = match std::str::from_utf8(data) {
        Ok(text) => Cow::Borrowed(text),
        Err(_) => {
            let mut text = String::with_capacity(data.len());
            let mut rest = data;
            while !rest.is_empty() {
                let valid = match std::str::from_utf8(rest) {
                    Ok(valid) => valid.len(),
                    Err(err) => err.valid_up_to(),
                };
                text.push_str(&String::from_utf8_lossy(&rest[..valid]));
                rest = &rest[valid..];
                if !rest.is_empty() {
                    text.push('\u{fffd}');
                    rest = &rest[1..];
                }
            }
            Cow::Owned(text)
        }
    };

    // The four digits of `\uXXXX` at `at`, when that is what is there.
    fn escape(bytes: &[u8], at: usize) -> Option<u32> {
        let escape = bytes.get(at..at + 6)?;
        if &escape[..2] != b"\\u" || !escape[2..].iter().all(u8::is_ascii_hexdigit) {
            return None;
        }
        u32::from_str_radix(std::str::from_utf8(&escape[2..]).ok()?, 16).ok()
    }

    // Where the escapes are that are half of a pair.
    let bytes = text.as_bytes();
    let mut halves = Vec::new();
    let mut in_string = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                in_string = !in_string;
                i += 1;
            }
            b'\\' if in_string => match escape(bytes, i) {
                Some(0xD800..=0xDBFF) if matches!(escape(bytes, i + 6), Some(0xDC00..=0xDFFF)) => {
                    i += 12;
                }
                Some(0xD800..=0xDFFF) => {
                    halves.push(i);
                    i += 6;
                }
                // Another escape: the backslash and what it escapes.
                _ => i += 2,
            },
            _ => i += 1,
        }
    }
    for at in halves {
        text.to_mut().replace_range(at + 2..at + 6, "fffd");
    }
    text
}

pub(super) fn from_value<T: DeserializeOwned>(value: Value) -> Result<T, Error> {
    T::deserialize(GoJson(value))
}

/// A JSON value that gives itself to serde by the rules above.
struct GoJson(Value);

impl<'de> IntoDeserializer<'de, Error> for GoJson {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self {
        self
    }
}

fn unexpected(value: &Value) -> Unexpected<'_> {
    match value {
        Value::Null => Unexpected::Unit,
        Value::Bool(b) => Unexpected::Bool(*b),
        Value::Number(n) => n
            .as_f64()
            .map_or(Unexpected::Other("number"), Unexpected::Float),
        Value::String(s) => Unexpected::Str(s),
        Value::Array(_) => Unexpected::Seq,
        Value::Object(_) => Unexpected::Map,
    }
}

fn visit_map<'de, V: Visitor<'de>>(
    entries: impl Iterator<Item = (String, Value)>,
    visitor: V,
) -> Result<V::Value, Error> {
    let mut map =
        MapDeserializer::<_, Error>::new(entries.map(|(key, value)| (key, GoJson(value))));
    let value = visitor.visit_map(&mut map)?;
    map.end()?;
    Ok(value)
}

// Whether two names are the same to Go's `encoding/json` when they are not equal: letters
// match in either case, and so do the two characters outside ASCII that Unicode folds to an
// ASCII letter.
fn equal_fold(field: &str, key: &str) -> bool {
    fn fold(c: char) -> char {
        match c {
            '\u{212a}' => 'k', // the Kelvin sign
            '\u{17f}' => 's',  // the long s
            c => c.to_ascii_lowercase(),
        }
    }
    field.chars().map(fold).eq(key.chars().map(fold))
}

impl<'de> Deserializer<'de> for GoJson {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self.0 {
            Value::Array(values) => {
                let mut seq = SeqDeserializer::<_, Error>::new(values.into_iter().map(GoJson));
                let value = visitor.visit_seq(&mut seq)?;
                seq.end()?;
                Ok(value)
            }
            Value::Object(entries) => visit_map(entries.into_iter(), visitor),
            // protojson takes a number that is written as a float for an integer when it
            // has no fraction (`5.0`, `1e1`). The readers of the protocol's messages take
            // an integer for a float but not a float for an integer, so such a number is
            // given to them as the integer it is.
            Value::Number(n) if n.is_f64() => match n.as_f64() {
                Some(f) if f.fract() == 0.0 && (0.0..18446744073709551616.0).contains(&f) => {
                    visitor.visit_u64(f as u64)
                }
                Some(f) if f.fract() == 0.0 && (-9223372036854775808.0..0.0).contains(&f) => {
                    visitor.visit_i64(f as i64)
                }
                _ => Value::Number(n).deserialize_any(visitor),
            },
            scalar => scalar.deserialize_any(visitor),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self.0 {
            Value::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_newtype_struct(self)
    }

    // A Go struct or a message of the protocol: serde hands over the names of its fields,
    // which is what the rules need.
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        // The messages of the protocol are named with their package
        // ("livekit.RoomConfiguration"), the Go structs are not.
        let go_struct = !name.contains('.');
        let entries = match self.0 {
            Value::Object(entries) => entries,
            // Go leaves a struct as it is for null. To protojson a field that is null is a
            // field that is not set (which is seen to below), but a message that is null,
            // in a list or in a map, is an error.
            Value::Null if go_struct => serde_json::Map::new(),
            // Not an array either, which serde would take as the fields in order.
            other => return Err(Error::invalid_type(unexpected(&other), &visitor)),
        };
        let mut known = BTreeMap::new();
        let mut in_another_case = Vec::new();
        for (key, value) in entries {
            if value.is_null() {
                continue;
            }
            if fields.contains(&key.as_str()) {
                known.insert(key, value);
            } else if go_struct {
                in_another_case.push((key, value));
            }
        }
        for (key, value) in in_another_case {
            if let Some(field) = fields.iter().find(|field| equal_fold(field, &key)) {
                known.entry(field.to_string()).or_insert(value);
            }
        }
        visit_map(known.into_iter(), visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.0.deserialize_enum(name, variants, visitor)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        unit unit_struct seq tuple tuple_struct map identifier ignored_any
    }
}

/// A list of strings, in which Go reads `null` as the empty string.
pub(super) fn string_list<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<String>, D::Error> {
    let list = Vec::<Option<String>>::deserialize(deserializer)?;
    Ok(list.into_iter().map(Option::unwrap_or_default).collect())
}

/// A map of strings, in which Go reads `null` as the empty string.
pub(super) fn string_map<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<HashMap<String, String>, D::Error> {
    let map = HashMap::<String, Option<String>>::deserialize(deserializer)?;
    Ok(map
        .into_iter()
        .map(|(key, value)| (key, value.unwrap_or_default()))
        .collect())
}

/// A map as Go writes it: with its keys in order.
pub(super) fn sorted_map<S: Serializer>(
    map: &HashMap<String, String>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_map(map.iter().collect::<BTreeMap<_, _>>())
}

/// For `omitempty` on a `bool`.
pub(super) fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::livekit;

    #[derive(Debug, Default, PartialEq, Deserialize)]
    #[serde(default)]
    struct Inner {
        #[serde(rename = "roomJoin")]
        room_join: bool,
        room: String,
        #[serde(rename = "canPublish")]
        can_publish: Option<bool>,
    }

    #[derive(Debug, Default, PartialEq, Deserialize)]
    #[serde(default)]
    struct Outer {
        name: String,
        video: Option<Inner>,
        #[serde(deserialize_with = "string_list")]
        list: Vec<String>,
        #[serde(deserialize_with = "string_map")]
        attributes: HashMap<String, String>,
        sk: String,
    }

    fn outer(text: &str) -> Result<Outer, Error> {
        from_slice(text.as_bytes())
    }

    #[test]
    fn null_leaves_a_field_as_it_is() {
        let got = outer(r#"{"name":null,"video":null,"list":null,"attributes":null}"#).unwrap();
        assert_eq!(got, Outer::default());

        let got = outer(r#"{"video":{"roomJoin":null,"room":null,"canPublish":null}}"#).unwrap();
        assert_eq!(got.video, Some(Inner::default()));

        // In a list and in a map it is the empty string.
        let got = outer(r#"{"list":[null,"a"],"attributes":{"a":null,"b":"c"}}"#).unwrap();
        assert_eq!(got.list, ["", "a"]);
        assert_eq!(got.attributes["a"], "");
        assert_eq!(got.attributes["b"], "c");

        // A struct that is null as a whole stays as it is too.
        assert_eq!(outer("null").unwrap(), Outer::default());
    }

    #[test]
    fn the_name_of_a_field_of_a_go_struct_matches_in_any_case() {
        let got = outer(r#"{"NAME":"n","Video":{"ROOMJOIN":true,"Room":"r","canpublish":false}}"#)
            .unwrap();
        assert_eq!(got.name, "n");
        assert_eq!(
            got.video,
            Some(Inner {
                room_join: true,
                room: "r".into(),
                can_publish: Some(false),
            })
        );

        // Another spelling is another name.
        let got = outer(r#"{"video":{"room_join":true}}"#).unwrap();
        assert_eq!(got.video, Some(Inner::default()));

        // The two characters outside ASCII that fold to an ASCII letter.
        let got = outer("{\"\u{17f}\u{212a}\":\"x\"}").unwrap();
        assert_eq!(got.sk, "x");

        // The exact name wins, wherever it stands.
        let got = outer(r#"{"Name":"other","name":"exact","NAME":"other"}"#).unwrap();
        assert_eq!(got.name, "exact");
    }

    #[test]
    fn what_is_not_known_is_left_out_and_what_has_the_wrong_type_is_an_error() {
        let got =
            outer(r#"{"name":"n","future":{"a":[1,{"b":null}]},"video":{"future":1}}"#).unwrap();
        assert_eq!(got.name, "n");
        assert_eq!(got.video, Some(Inner::default()));

        for text in [
            r#"{"name":5}"#,
            r#"{"video":"yes"}"#,
            r#"{"video":[true,"r",false]}"#,
            r#"{"video":{"roomJoin":"true"}}"#,
            r#"{"video":{"roomJoin":1}}"#,
            r#"{"list":"a"}"#,
            r#"{"attributes":{"a":5}}"#,
            r#"[]"#,
            r#""x""#,
            r#"{"#,
            r#"{} x"#,
        ] {
            assert!(outer(text).is_err(), "{text}");
        }
    }

    #[test]
    fn a_message_of_the_protocol_is_read_as_protojson_reads_it() {
        let config = |text: &str| from_slice::<livekit::RoomConfiguration>(text.as_bytes());

        // What it does not know is left out, at any depth.
        let got = config(
            r#"{"name":"r","future":1,"egress":{"future":{"a":1},"room":{"roomName":"r","more":[]}},
                "agents":[{"agentName":"a","future":null}]}"#,
        )
        .unwrap();
        assert_eq!(got.name, "r");
        assert_eq!(got.egress.unwrap().room.unwrap().room_name, "r");
        assert_eq!(got.agents[0].agent_name, "a");

        // Both names of a field, and a number in quotes.
        let got = config(r#"{"empty_timeout":5,"maxParticipants":"7"}"#).unwrap();
        assert_eq!((got.empty_timeout, got.max_participants), (5, 7));

        // null is a field that is not set.
        let got =
            config(r#"{"name":null,"emptyTimeout":null,"egress":null,"agents":null}"#).unwrap();
        assert_eq!(got, livekit::RoomConfiguration::default());

        // A name in another case is a name it does not know.
        let got = config(r#"{"Name":"r","EmptyTimeout":5}"#).unwrap();
        assert_eq!(got, livekit::RoomConfiguration::default());

        // A map keeps all its keys.
        let got = config(r#"{"tags":{"future":"x","name":"y"}}"#).unwrap();
        assert_eq!(got.tags.len(), 2);

        // A number without a fraction is an integer, however it is written.
        let got = config(r#"{"emptyTimeout":5.0,"maxParticipants":1e1}"#).unwrap();
        assert_eq!((got.empty_timeout, got.max_participants), (5, 10));
        assert!(config(r#"{"emptyTimeout":5.5}"#).is_err());
        assert!(config(r#"{"emptyTimeout":-1.0}"#).is_err());
        assert!(config(r#"{"emptyTimeout":1e10}"#).is_err());

        assert!(config(r#"{"name":5}"#).is_err());
        assert!(config(r#""x""#).is_err());
        assert!(config(r#"["r"]"#).is_err());
        // A message that is null is an error where a field that is null is not.
        assert!(config("null").is_err());
        assert!(config(r#"{"agents":[null]}"#).is_err());
        assert!(config(r#"{"tags":{"a":null}}"#).is_err());
    }

    #[test]
    fn text_that_is_not_unicode_is_read_as_go_reads_it() {
        // An escape as JSON has it: a backslash, "u" and four digits.
        let bs = '\\';
        let u = |digits: &str| format!("{bs}u{digits}");
        let (high, low, half, other) = (u("d83d"), u("de00"), u("d800"), u("0041"));
        let replaced = u("fffd");
        let text = |text: String| go_text(text.as_bytes()).into_owned();

        // Text that is as it should be is not copied.
        let good = format!("{{\"a\":\"b{high}{low}{other}\"}}");
        assert!(matches!(go_text(good.as_bytes()), Cow::Borrowed(_)));
        assert_eq!(go_text(b""), "");

        // One U+FFFD for each byte that is not UTF-8.
        assert_eq!(go_text(b"\"a\xffb\""), "\"a\u{fffd}b\"");
        assert_eq!(go_text(b"\"a\xe2\x82b\""), "\"a\u{fffd}\u{fffd}b\"");
        assert_eq!(
            go_text(b"\xf0\x9f\x98\"\xc3\xa9\xff"),
            "\u{fffd}\u{fffd}\u{fffd}\"\u{e9}\u{fffd}"
        );

        // An escape that is half of a surrogate pair, in a string.
        assert_eq!(text(format!("\"a{half}b\"")), format!("\"a{replaced}b\""));
        assert_eq!(
            text(format!("\"a{}b\"", u("DFFF"))),
            format!("\"a{replaced}b\"")
        );
        assert_eq!(text(format!("\"{high}\"")), format!("\"{replaced}\""));
        assert_eq!(
            text(format!("\"{high}{other}\"")),
            format!("\"{replaced}{other}\"")
        );
        assert_eq!(
            text(format!("\"{low}{high}\"")),
            format!("\"{replaced}{replaced}\"")
        );
        assert_eq!(
            text(format!("\"{high}{high}{low}\"")),
            format!("\"{replaced}{high}{low}\"")
        );
        assert_eq!(
            text(format!("{{\"{high}\":\"{low}\"}}")),
            format!("{{\"{replaced}\":\"{replaced}\"}}")
        );
        // After a backslash that is escaped itself there is an escape, after two there is
        // none.
        assert_eq!(
            text(format!("\"a{bs}{bs}{half}b\"")),
            format!("\"a{bs}{bs}{replaced}b\"")
        );
        // What is not such an escape stays as it is.
        for unchanged in [
            format!("\"a{high}{low}b\""),
            format!("\"a{}{}b\"", u("D83D"), u("DE00")),
            format!("\"a{bs}{half}b\""),
            format!("\"a{bs}{bs}{bs}{half}b\""),
            format!("\"{bs}\"{bs}{half}\""),
            format!("\"{}{}\"", u("d7ff"), u("e000")),
            format!("\"{bs}ud80\""),
            format!("\"{bs}ud80g\""),
            format!("\"{bs}u+d80\""),
            // Outside a string it is not JSON, and not looked at.
            half.clone(),
            format!("{{\"a\":\"b\"}}{half}"),
        ] {
            assert_eq!(text(unchanged.clone()), unchanged);
        }

        // Read, it is the character that stands for what is not Unicode.
        let name = |text: &[u8]| from_slice::<Outer>(text).unwrap().name;
        assert_eq!(name(b"{\"name\":\"a\xffb\"}"), "a\u{fffd}b");
        assert_eq!(
            name(format!("{{\"name\":\"a{half}b\"}}").as_bytes()),
            "a\u{fffd}b"
        );
        assert_eq!(
            name(format!("{{\"name\":\"a{high}{low}b\"}}").as_bytes()),
            "a\u{1f600}b"
        );
        assert_eq!(
            name(format!("{{\"na{half}me\":\"x\",\"name\":\"y\"}}").as_bytes()),
            "y"
        );
    }

    #[test]
    fn a_map_is_written_with_its_keys_in_order() {
        #[derive(serde::Serialize)]
        struct Attributes {
            #[serde(serialize_with = "sorted_map")]
            attributes: HashMap<String, String>,
        }
        let attributes = ["k3", "k1", "k4", "k2", "k0"]
            .into_iter()
            .map(|key| (key.to_string(), "v".to_string()))
            .collect();
        assert_eq!(
            serde_json::to_string(&Attributes { attributes }).unwrap(),
            r#"{"attributes":{"k0":"v","k1":"v","k2":"v","k3":"v","k4":"v"}}"#
        );
    }
}
