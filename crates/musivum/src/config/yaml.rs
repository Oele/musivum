// Follows gopkg.in/yaml.v3 at v3.0.1, decode.go and resolve.go: what that decoder does when
// LiveKit reads its configuration into Go values.
// Copyright (c) 2011-2019 Canonical Ltd. Licensed under the Apache License, Version 2.0.
// Changes: written in Rust, on the events of the crate `saphyr-parser` where the original has
// a parser of its own, and reduced to the kinds of values a configuration has. Go's
// reflection is the trait `Yaml`: a type says how a node is decoded into a value of it that
// exists already, which is how LiveKit lays a file over its defaults (a struct keeps the
// fields the file does not name, a map keeps its other keys and gets new values, a slice is
// replaced). A struct gets the trait from `yaml_struct!`, which lists its fields with their
// names in YAML. The texts of the type errors are the original's, with Go's names for the
// types (an `i64` is called `int`, which is what most of them are there). Left out:
// timestamps (a date is a string here, which only changes the text of an error), and the
// decoding into `interface{}` but for what `unmarshal_keys` needs. An alias is replaced by
// a copy of what it names when the document is read, and a merge key (`<<`) is resolved
// then too. What the parser refuses is the parser's, so the texts of syntax errors are not
// the original's.

use std::collections::HashMap;
use std::hash::Hash;
use std::iter::Peekable;

use base64::Engine as _;
use saphyr_parser::{Event, Parser, ScalarStyle, ScanError, Span, Tag};

use crate::go::time::Duration;
use crate::mediatransportutil::rtcconfig::parse_duration;

pub const NULL_TAG: &str = "!!null";
pub const BOOL_TAG: &str = "!!bool";
pub const STR_TAG: &str = "!!str";
pub const INT_TAG: &str = "!!int";
pub const FLOAT_TAG: &str = "!!float";
pub const TIMESTAMP_TAG: &str = "!!timestamp";
pub const SEQ_TAG: &str = "!!seq";
pub const MAP_TAG: &str = "!!map";
pub const BINARY_TAG: &str = "!!binary";
pub const MERGE_TAG: &str = "!!merge";

// The nodes that aliases may add to a document. The original bounds the share of the
// decoding that comes from aliases; a configuration needs nothing near either bound.
const MAX_ALIASED_NODES: usize = 100_000;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// Go's `io.EOF`, which `Decoder.Decode` gives when there is no document.
    #[error("EOF")]
    Eof,
    /// What the parser refuses, and what the original stops at with `failf`.
    #[error("yaml: {0}")]
    Yaml(String),
    /// The original's `TypeError`: everything in the document that does not fit the value.
    #[error("yaml: unmarshal errors:\n  {}", .0.join("\n  "))]
    Type(Vec<String>),
    /// The error of an `UnmarshalYAML`, which the original hands on as it is.
    #[error("{0}")]
    Unmarshaler(String),
}

// ----------------------------------------------------------------------------
// The document.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Scalar,
    Mapping,
    Sequence,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    pub kind: Kind,
    /// The tag in its short form (`!!str`): the one that is written, or else `!!map`,
    /// `!!seq`, `!!str` for a scalar in quotes, and what a plain scalar resolves to.
    pub tag: String,
    /// Whether the tag is written in the document.
    pub tagged: bool,
    /// The text of a scalar.
    pub value: String,
    /// The items of a sequence; the keys and the values of a mapping, in turns.
    pub content: Vec<Node>,
    pub line: usize,
}

impl Node {
    /// The original's `n.ShortTag() == nullTag`.
    pub fn is_null(&self) -> bool {
        self.tag == NULL_TAG
    }

    fn count(&self) -> usize {
        1 + self.content.iter().map(Node::count).sum::<usize>()
    }
}

/// The first document of `text`, or nothing if there is none.
pub fn parse(text: &str) -> Result<Option<Node>, Error> {
    let mut builder = Builder {
        events: Parser::new_from_str(text).peekable(),
        anchors: HashMap::new(),
        aliased: 0,
    };
    loop {
        match builder.next()? {
            (Event::DocumentStart(_), _) => break,
            (Event::StreamEnd, _) => return Ok(None),
            _ => {}
        }
    }
    builder.node().map(Some)
}

struct Builder<I: Iterator> {
    events: Peekable<I>,
    // What was given a name (`&name`), for the aliases to it (`*name`).
    anchors: HashMap<usize, Node>,
    aliased: usize,
}

impl<'a, I: Iterator<Item = Result<(Event<'a>, Span), ScanError>>> Builder<I> {
    fn next(&mut self) -> Result<(Event<'a>, Span), Error> {
        match self.events.next() {
            Some(Ok(event)) => Ok(event),
            Some(Err(err)) => Err(syntax_error(&err)),
            None => Ok((Event::StreamEnd, Span::default())),
        }
    }

    fn at_end(&mut self, end: &Event<'_>) -> Result<bool, Error> {
        match self.events.peek() {
            Some(Ok((event, _))) => Ok(event == end || *event == Event::StreamEnd),
            Some(Err(err)) => Err(syntax_error(err)),
            None => Ok(true),
        }
    }

    fn node(&mut self) -> Result<Node, Error> {
        let (event, span) = self.next()?;
        let line = span.start.line();
        let (node, anchor) = match event {
            Event::Scalar(value, style, anchor, tag) => {
                let (tag, tagged) = match short_tag(tag.as_deref()) {
                    Some(tag) => (tag, true),
                    None if style != ScalarStyle::Plain => (STR_TAG.to_string(), false),
                    None if value == "<<" => (MERGE_TAG.to_string(), false),
                    None => (resolve(&value).tag().to_string(), false),
                };
                let node = Node {
                    kind: Kind::Scalar,
                    tag,
                    tagged,
                    value: value.into_owned(),
                    content: Vec::new(),
                    line,
                };
                (node, anchor)
            }
            Event::SequenceStart(anchor, tag) => {
                let mut content = Vec::new();
                while !self.at_end(&Event::SequenceEnd)? {
                    content.push(self.node()?);
                }
                self.next()?;
                let tag = short_tag(tag.as_deref());
                let node = Node {
                    kind: Kind::Sequence,
                    tagged: tag.is_some(),
                    tag: tag.unwrap_or_else(|| SEQ_TAG.to_string()),
                    value: String::new(),
                    content,
                    line,
                };
                (node, anchor)
            }
            Event::MappingStart(anchor, tag) => {
                let mut content = Vec::new();
                while !self.at_end(&Event::MappingEnd)? {
                    content.push(self.node()?);
                    content.push(self.node()?);
                }
                self.next()?;
                let tag = short_tag(tag.as_deref());
                let node = Node {
                    kind: Kind::Mapping,
                    tagged: tag.is_some(),
                    tag: tag.unwrap_or_else(|| MAP_TAG.to_string()),
                    value: String::new(),
                    content: merge(content)?,
                    line,
                };
                (node, anchor)
            }
            Event::Alias(anchor) => {
                let Some(node) = self.anchors.get(&anchor) else {
                    // an anchor that names what it is part of
                    return Err(Error::Yaml("anchor value contains itself".to_string()));
                };
                self.aliased += node.count();
                if self.aliased > MAX_ALIASED_NODES {
                    return Err(Error::Yaml(
                        "document contains excessive aliasing".to_string(),
                    ));
                }
                return Ok(node.clone());
            }
            // a document with nothing in it
            _ => {
                let node = Node {
                    kind: Kind::Scalar,
                    tag: NULL_TAG.to_string(),
                    tagged: false,
                    value: String::new(),
                    content: Vec::new(),
                    line,
                };
                (node, 0)
            }
        };
        if anchor > 0 {
            self.anchors.insert(anchor, node.clone());
        }
        Ok(node)
    }
}

fn syntax_error(err: &ScanError) -> Error {
    Error::Yaml(format!("line {}: {}", err.marker().line(), err.info()))
}

fn short_tag(tag: Option<&Tag>) -> Option<String> {
    let tag = tag?;
    if tag.is_yaml_core_schema() {
        return Some(format!("!!{}", tag.suffix));
    }
    if tag.handle == "!" && tag.suffix.is_empty() {
        return None;
    }
    Some(format!("{}{}", tag.handle, tag.suffix))
}

fn is_merge(n: &Node) -> bool {
    n.kind == Kind::Scalar && n.value == "<<" && n.tag == MERGE_TAG
}

fn fail_want_map() -> Error {
    Error::Yaml("map merge requires map or sequence of maps as the value".to_string())
}

// The keys and values of a mapping with what its merge key (`<<`) names laid under them: a
// key of the mapping itself wins, then the first mapping that has it. The original does
// this while it decodes, the mapping's own keys first; the outcome is the same.
fn merge(content: Vec<Node>) -> Result<Vec<Node>, Error> {
    if !content.chunks(2).any(|pair| is_merge(&pair[0])) {
        return Ok(content);
    }

    let mut out = Vec::with_capacity(content.len());
    let mut merge_node = None;
    let mut pairs = content.into_iter();
    while let (Some(key), Some(value)) = (pairs.next(), pairs.next()) {
        if is_merge(&key) {
            merge_node = Some(value);
            continue;
        }
        out.push(key);
        out.push(value);
    }

    let Some(merge_node) = merge_node else {
        return Ok(out);
    };
    let sources = match merge_node.kind {
        Kind::Mapping => vec![merge_node],
        Kind::Sequence => {
            if merge_node.content.iter().any(|n| n.kind != Kind::Mapping) {
                return Err(fail_want_map());
            }
            merge_node.content
        }
        Kind::Scalar => return Err(fail_want_map()),
    };
    for source in sources {
        let mut pairs = source.content.into_iter();
        while let (Some(key), Some(value)) = (pairs.next(), pairs.next()) {
            let there = out
                .chunks(2)
                .any(|pair| pair[0].kind == key.kind && pair[0].value == key.value);
            if !there {
                out.push(key);
                out.push(value);
            }
        }
    }
    Ok(out)
}

// ----------------------------------------------------------------------------
// What a scalar is.

/// What the text of a scalar stands for. A string has its text in the node.
#[derive(Clone, Debug, PartialEq)]
pub enum Resolved {
    Null,
    Bool(bool),
    Int(i64),
    /// A whole number above the largest `i64`.
    Uint(u64),
    Float(f64),
    Str,
    /// The bytes of a `!!binary`, as text.
    Binary(String),
}

impl Resolved {
    pub fn tag(&self) -> &'static str {
        match self {
            Resolved::Null => NULL_TAG,
            Resolved::Bool(_) => BOOL_TAG,
            Resolved::Int(_) | Resolved::Uint(_) => INT_TAG,
            Resolved::Float(_) => FLOAT_TAG,
            Resolved::Str => STR_TAG,
            Resolved::Binary(_) => BINARY_TAG,
        }
    }
}

/// The original's `resolve("", in)`: what a plain scalar without a tag is.
pub fn resolve(value: &str) -> Resolved {
    // the prefix is enough of a hint about what it might be
    let hint = match value.as_bytes().first() {
        None => b'N',
        Some(b'+' | b'-') => b'S', // Sign
        Some(b'0'..=b'9') => b'D', // Digit
        Some(b'y' | b'Y' | b'n' | b'N' | b't' | b'T' | b'f' | b'F' | b'o' | b'O' | b'~') => b'M', // In map
        Some(b'.') => b'.', // Float (potentially in map)
        Some(_) => return Resolved::Str,
    };

    // Handle things we can lookup in a map.
    match value {
        "true" | "True" | "TRUE" => return Resolved::Bool(true),
        "false" | "False" | "FALSE" => return Resolved::Bool(false),
        "" | "~" | "null" | "Null" | "NULL" => return Resolved::Null,
        ".nan" | ".NaN" | ".NAN" => return Resolved::Float(f64::NAN),
        ".inf" | ".Inf" | ".INF" | "+.inf" | "+.Inf" | "+.INF" => {
            return Resolved::Float(f64::INFINITY);
        }
        "-.inf" | "-.Inf" | "-.INF" => return Resolved::Float(f64::NEG_INFINITY),
        _ => {}
    }

    match hint {
        b'.' => {
            // Not in the map, so maybe a normal float.
            if let Some(floatv) = parse_float(value) {
                return Resolved::Float(floatv);
            }
        }
        b'D' | b'S' => {
            // Int or float.
            let plain = value.replace('_', "");
            if let Some(intv) = parse_int(&plain) {
                return intv;
            }
            if is_yaml_style_float(&plain)
                && let Some(floatv) = parse_float(&plain)
            {
                return Resolved::Float(floatv);
            }
        }
        _ => {}
    }
    Resolved::Str
}

// Go's `strconv.ParseInt(s, 0, 64)` and, for what is too large for it, `ParseUint`: a sign,
// then `0x`, `0b` or `0o` and digits of that base, or a `0` and octal digits, or decimal
// digits.
fn parse_int(s: &str) -> Option<Resolved> {
    let (sign, digits) = match s.as_bytes().first()? {
        b'+' | b'-' => s.split_at(1),
        _ => ("", s),
    };
    let prefixed = |prefix: &str| {
        digits
            .get(..2)
            .filter(|p| p.eq_ignore_ascii_case(prefix))
            .map(|_| &digits[2..])
    };
    let (base, digits) = if let Some(rest) = prefixed("0x") {
        (16, rest)
    } else if let Some(rest) = prefixed("0b") {
        (2, rest)
    } else if let Some(rest) = prefixed("0o") {
        (8, rest)
    } else if digits.len() > 1 && digits.starts_with('0') {
        (8, &digits[1..])
    } else {
        (10, digits)
    };
    if digits.is_empty() || !digits.chars().all(|c| c.is_digit(base)) {
        return None;
    }
    let magnitude = u64::from_str_radix(digits, base).ok()?;
    if sign == "-" {
        return 0i64.checked_sub_unsigned(magnitude).map(Resolved::Int);
    }
    match i64::try_from(magnitude) {
        Ok(intv) => Some(Resolved::Int(intv)),
        // ParseUint takes no sign
        Err(_) if sign.is_empty() => Some(Resolved::Uint(magnitude)),
        Err(_) => None,
    }
}

// `strconv.ParseFloat` for the decimal numbers that get here; one that is out of range is
// an error there and an infinity for Rust.
fn parse_float(s: &str) -> Option<f64> {
    s.parse::<f64>().ok().filter(|f| f.is_finite())
}

// The original's `yamlStyleFloat`:
// `^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$`
fn is_yaml_style_float(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    let digits = |i: &mut usize| {
        let start = *i;
        while *i < b.len() && b[*i].is_ascii_digit() {
            *i += 1;
        }
        *i - start
    };
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        i += 1;
    }
    if i < b.len() && b[i] == b'.' {
        i += 1;
        if digits(&mut i) == 0 {
            return false;
        }
    } else {
        if digits(&mut i) == 0 {
            return false;
        }
        if i < b.len() && b[i] == b'.' {
            i += 1;
            digits(&mut i);
        }
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        if digits(&mut i) == 0 {
            return false;
        }
    }
    i == b.len()
}

// ----------------------------------------------------------------------------
// Decoder, unmarshals a node into a provided value.

/// A Go value that yaml.v3 decodes into.
pub trait Yaml {
    /// What Go calls the type, for the messages.
    fn type_name() -> String;

    /// The original's `unmarshal(n, out)` for a value of this type: lays the node over the
    /// value that is there. Says whether the value was set; what does not fit is in the
    /// decoder's errors.
    fn unmarshal(&mut self, d: &mut Decoder, n: &Node) -> bool;
}

/// A Go struct: its fields by their names in YAML.
pub trait Struct: Yaml {
    /// Decodes `value` into the field `name`. Says whether there is such a field, in the
    /// struct or in one that is inline in it.
    fn unmarshal_field(&mut self, d: &mut Decoder, name: &str, value: &Node) -> bool;
}

pub struct Decoder {
    known_fields: bool,
    terrors: Vec<String>,
    // what the original stops at (`fail`, `failf`)
    failed: Option<Error>,
}

/// `yaml.NewDecoder(r)`, `KnownFields(known_fields)` and `Decode(out)`: the first document
/// of `text` over `out`.
pub fn decode<T: Yaml>(text: &str, known_fields: bool, out: &mut T) -> Result<(), Error> {
    let Some(node) = parse(text)? else {
        return Err(Error::Eof);
    };
    let mut d = Decoder::new(known_fields);
    out.unmarshal(&mut d, &node);
    d.finish()
}

/// `yaml.Unmarshal(in, out)`: as `decode` without `KnownFields`, and text without a
/// document is no error.
pub fn unmarshal<T: Yaml>(text: &str, out: &mut T) -> Result<(), Error> {
    match decode(text, false, out) {
        Err(Error::Eof) => Ok(()),
        result => result,
    }
}

impl Decoder {
    pub fn new(known_fields: bool) -> Decoder {
        Decoder {
            known_fields,
            terrors: Vec::new(),
            failed: None,
        }
    }

    pub fn finish(self) -> Result<(), Error> {
        if let Some(err) = self.failed {
            return Err(err);
        }
        if !self.terrors.is_empty() {
            return Err(Error::Type(self.terrors));
        }
        Ok(())
    }

    pub fn terror(&mut self, n: &Node, type_name: &str) {
        let mut value = String::new();
        if n.tag != SEQ_TAG && n.tag != MAP_TAG {
            value = if n.value.len() > 10 {
                format!(
                    " `{}...`",
                    String::from_utf8_lossy(&n.value.as_bytes()[..7])
                )
            } else {
                format!(" `{}`", n.value)
            };
        }
        self.terrors.push(format!(
            "line {}: cannot unmarshal {}{} into {}",
            n.line, n.tag, value, type_name
        ));
    }

    /// The original's `fail(err)` in `callUnmarshaler`: decoding stops with this error.
    pub fn fail(&mut self, err: impl ToString) {
        if self.failed.is_none() {
            self.failed = Some(Error::Unmarshaler(err.to_string()));
        }
    }

    fn failf(&mut self, message: String) {
        if self.failed.is_none() {
            self.failed = Some(Error::Yaml(message));
        }
    }

    pub fn has_failed(&self) -> bool {
        self.failed.is_some()
    }

    /// What a scalar node is, by its tag and its text. Nothing if the two contradict each
    /// other, which stops the decoding.
    pub fn resolve(&mut self, n: &Node) -> Option<Resolved> {
        if n.tag == STR_TAG {
            return Some(Resolved::Str);
        }
        if !n.tagged {
            return Some(resolve(&n.value));
        }
        match n.tag.as_str() {
            BINARY_TAG => {
                // Go's decoder of base64 skips line breaks.
                let data = base64::engine::general_purpose::STANDARD
                    .decode(n.value.replace(['\r', '\n'], ""))
                    .ok()
                    .and_then(|data| String::from_utf8(data).ok());
                if data.is_none() {
                    self.failf("!!binary value contains invalid base64 data".to_string());
                }
                data.map(Resolved::Binary)
            }
            NULL_TAG | BOOL_TAG | INT_TAG | FLOAT_TAG | TIMESTAMP_TAG => {
                match (n.tag.as_str(), resolve(&n.value)) {
                    (tag, resolved) if resolved.tag() == tag => Some(resolved),
                    (FLOAT_TAG, Resolved::Int(intv)) => Some(Resolved::Float(intv as f64)),
                    (tag, resolved) => {
                        self.failf(format!(
                            "cannot decode {} `{}` as a {}",
                            resolved.tag(),
                            n.value,
                            tag
                        ));
                        None
                    }
                }
            }
            // a tag of the document's own: the text
            _ => Some(Resolved::Str),
        }
    }

    /// What a value that is not made of others (a number, a string, a duration) is given:
    /// nothing for a null, which leaves such a value as it is, and for a mapping or a
    /// sequence, which is an error.
    pub fn scalar<T: Yaml>(&mut self, n: &Node) -> Option<Resolved> {
        match n.kind {
            Kind::Scalar => self.resolve(n).filter(|r| *r != Resolved::Null),
            Kind::Mapping => {
                if self.unique_keys(n) {
                    self.terror(n, &T::type_name());
                }
                None
            }
            Kind::Sequence => {
                self.terror(n, &T::type_name());
                None
            }
        }
    }

    /// Whether a node that a slice, a map or a pointer is decoded from is a null, which
    /// empties it. A scalar that is something else is an error and gives nothing.
    fn null<T: Yaml>(&mut self, n: &Node) -> Option<bool> {
        match self.resolve(n)? {
            Resolved::Null => Some(true),
            _ => {
                self.terror(n, &T::type_name());
                Some(false)
            }
        }
    }

    // The start of the original's `mapping`: no key twice.
    fn unique_keys(&mut self, n: &Node) -> bool {
        let nerrs = self.terrors.len();
        for (i, ni) in n.content.iter().enumerate().step_by(2) {
            for nj in n.content.iter().skip(i + 2).step_by(2) {
                if ni.kind == nj.kind && ni.value == nj.value {
                    self.terrors.push(format!(
                        "line {}: mapping key {:?} already defined at line {}",
                        nj.line, nj.value, ni.line
                    ));
                }
            }
        }
        self.terrors.len() == nerrs
    }

    /// The original's `mappingStruct`, with what `unmarshal` and `mapping` do before it.
    pub fn mapping_struct<T: Struct>(&mut self, out: &mut T, n: &Node) -> bool {
        match n.kind {
            Kind::Scalar => {
                // a null leaves the struct as it is
                if self.resolve(n).is_some_and(|r| r != Resolved::Null) {
                    self.terror(n, &T::type_name());
                }
                return false;
            }
            Kind::Sequence => {
                self.terror(n, &T::type_name());
                return false;
            }
            Kind::Mapping => {}
        }
        if !self.unique_keys(n) {
            return false;
        }

        for pair in n.content.chunks(2) {
            let (ni, value) = (&pair[0], &pair[1]);
            let mut name = String::new();
            if !name.unmarshal(self, ni) {
                continue;
            }
            if !out.unmarshal_field(self, &name, value) && self.known_fields {
                self.terrors.push(format!(
                    "line {}: field {} not found in type {}",
                    ni.line,
                    name,
                    T::type_name()
                ));
            }
            if self.has_failed() {
                break;
            }
        }
        true
    }
}

impl Yaml for bool {
    fn type_name() -> String {
        "bool".to_string()
    }

    fn unmarshal(&mut self, d: &mut Decoder, n: &Node) -> bool {
        let Some(resolved) = d.scalar::<Self>(n) else {
            return false;
        };
        match resolved {
            Resolved::Bool(b) => {
                *self = b;
                return true;
            }
            // This offers some compatibility with the 1.1 spec (https://yaml.org/type/bool.html).
            // It only works if explicitly attempting to unmarshal into a typed bool value.
            Resolved::Str => match n.value.as_str() {
                "y" | "Y" | "yes" | "Yes" | "YES" | "on" | "On" | "ON" => {
                    *self = true;
                    return true;
                }
                "n" | "N" | "no" | "No" | "NO" | "off" | "Off" | "OFF" => {
                    *self = false;
                    return true;
                }
                _ => {}
            },
            _ => {}
        }
        d.terror(n, &Self::type_name());
        false
    }
}

impl Yaml for String {
    fn type_name() -> String {
        "string".to_string()
    }

    // any scalar but a null, as it is written
    fn unmarshal(&mut self, d: &mut Decoder, n: &Node) -> bool {
        match d.scalar::<Self>(n) {
            Some(Resolved::Binary(text)) => *self = text,
            Some(_) => self.clone_from(&n.value),
            None => return false,
        }
        true
    }
}

macro_rules! yaml_int {
    ($($ty:ty => $name:literal),* $(,)?) => {$(
        impl Yaml for $ty {
            fn type_name() -> String {
                $name.to_string()
            }

            fn unmarshal(&mut self, d: &mut Decoder, n: &Node) -> bool {
                let Some(resolved) = d.scalar::<Self>(n) else {
                    return false;
                };
                let value = match resolved {
                    Resolved::Int(intv) => <$ty>::try_from(intv).ok(),
                    Resolved::Uint(uintv) => <$ty>::try_from(uintv).ok(),
                    // Go cuts the fraction off, and what does not fit after that is an error
                    Resolved::Float(floatv) if floatv.is_nan() => None,
                    Resolved::Float(floatv) if floatv < 0.0 => <$ty>::try_from(floatv as i64).ok(),
                    Resolved::Float(floatv) => <$ty>::try_from(floatv as u64).ok(),
                    _ => None,
                };
                match value {
                    Some(value) => {
                        *self = value;
                        true
                    }
                    None => {
                        d.terror(n, &Self::type_name());
                        false
                    }
                }
            }
        }
    )*};
}

// A `usize` is where the port has one for an `int` of the original that counts something:
// a negative number, which Go would take, is an error here.
yaml_int!(
    i32 => "int32", i64 => "int", u8 => "uint8", u16 => "uint16", u32 => "uint32",
    u64 => "uint64", usize => "int",
);

macro_rules! yaml_float {
    ($($ty:ty => $name:literal),* $(,)?) => {$(
        impl Yaml for $ty {
            fn type_name() -> String {
                $name.to_string()
            }

            fn unmarshal(&mut self, d: &mut Decoder, n: &Node) -> bool {
                let Some(resolved) = d.scalar::<Self>(n) else {
                    return false;
                };
                *self = match resolved {
                    Resolved::Int(intv) => intv as $ty,
                    Resolved::Uint(uintv) => uintv as $ty,
                    Resolved::Float(floatv) => floatv as $ty,
                    _ => {
                        d.terror(n, &Self::type_name());
                        return false;
                    }
                };
                true
            }
        }
    )*};
}

yaml_float!(f32 => "float32", f64 => "float64");

// A `time.Duration`: from a string that `time.ParseDuration` takes ("5s"). A number is
// refused: "This used to work in v2, but it's very unfriendly."
impl Yaml for Duration {
    fn type_name() -> String {
        "time.Duration".to_string()
    }

    fn unmarshal(&mut self, d: &mut Decoder, n: &Node) -> bool {
        let Some(resolved) = d.scalar::<Self>(n) else {
            return false;
        };
        if resolved == Resolved::Str
            && let Some(duration) = parse_duration(&n.value)
        {
            *self = duration;
            return true;
        }
        d.terror(n, &Self::type_name());
        false
    }
}

// A slice: a new one of the items that fit, each decoded into a zero value.
impl<T: Yaml + Default> Yaml for Vec<T> {
    fn type_name() -> String {
        format!("[]{}", T::type_name())
    }

    fn unmarshal(&mut self, d: &mut Decoder, n: &Node) -> bool {
        match n.kind {
            Kind::Scalar => {
                let null = d.null::<Self>(n) == Some(true);
                if null {
                    self.clear();
                }
                null
            }
            Kind::Mapping => {
                if d.unique_keys(n) {
                    d.terror(n, &Self::type_name());
                }
                false
            }
            Kind::Sequence => {
                let mut out = Vec::with_capacity(n.content.len());
                for item in &n.content {
                    let mut e = T::default();
                    if e.unmarshal(d, item) {
                        out.push(e);
                    }
                    if d.has_failed() {
                        break;
                    }
                }
                *self = out;
                true
            }
        }
    }
}

// A map: the keys of the node are set in the map that is there, each to a value decoded
// into a zero value; the other keys stay.
impl<K: Yaml + Default + Eq + Hash, V: Yaml + Default> Yaml for HashMap<K, V> {
    fn type_name() -> String {
        format!("map[{}]{}", K::type_name(), V::type_name())
    }

    fn unmarshal(&mut self, d: &mut Decoder, n: &Node) -> bool {
        match n.kind {
            Kind::Scalar => {
                let null = d.null::<Self>(n) == Some(true);
                if null {
                    self.clear();
                }
                return null;
            }
            Kind::Sequence => {
                d.terror(n, &Self::type_name());
                return false;
            }
            Kind::Mapping => {}
        }
        if !d.unique_keys(n) {
            return false;
        }

        for pair in n.content.chunks(2) {
            let mut k = K::default();
            if k.unmarshal(d, &pair[0]) {
                let mut e = V::default();
                if e.unmarshal(d, &pair[1]) || pair[1].is_null() && !self.contains_key(&k) {
                    self.insert(k, e);
                }
            }
            if d.has_failed() {
                break;
            }
        }
        true
    }
}

// A pointer: nil for a null, and else what it points to, made if there is nothing yet.
impl<T: Yaml + Default> Yaml for Option<T> {
    fn type_name() -> String {
        T::type_name()
    }

    fn unmarshal(&mut self, d: &mut Decoder, n: &Node) -> bool {
        if n.is_null() {
            *self = None;
            return true;
        }
        self.get_or_insert_with(T::default).unmarshal(d, n)
    }
}

/// `any` of the original as far as `unmarshal_keys` reads it: the text of what is a string
/// there, and nothing for all else.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AnyString(pub Option<String>);

impl Yaml for AnyString {
    fn type_name() -> String {
        "interface {}".to_string()
    }

    fn unmarshal(&mut self, d: &mut Decoder, n: &Node) -> bool {
        self.0 = None;
        if n.kind == Kind::Scalar {
            match d.resolve(n) {
                Some(Resolved::Str) => self.0 = Some(n.value.clone()),
                Some(Resolved::Binary(text)) => self.0 = Some(text),
                Some(_) => {}
                None => return false,
            }
        }
        true
    }
}

/// A part of LiveKit's configuration that has no effect in musivum: any YAML is taken for
/// it and kept as it is read, so that a file written for LiveKit loads.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IgnoredSection(pub Option<Node>);

impl IgnoredSection {
    /// Whether the file has the section, with something in it.
    pub fn is_present(&self) -> bool {
        self.0.as_ref().is_some_and(|n| !n.is_null())
    }
}

impl Yaml for IgnoredSection {
    fn type_name() -> String {
        "config.IgnoredSection".to_string()
    }

    fn unmarshal(&mut self, _d: &mut Decoder, n: &Node) -> bool {
        self.0 = Some(n.clone());
        true
    }
}

/// Makes a struct one that YAML is decoded into: `yaml_struct!(Type, "Go's name of the
/// type", { field: "name in YAML", embedded: inline, not_in_yaml: skip })`. Every field
/// has to be listed, so that one that is added to the struct cannot be forgotten here.
macro_rules! yaml_struct {
    ($ty:ty, $go:literal, { $($field:ident : $name:tt),* $(,)? }) => {
        impl $crate::config::yaml::Yaml for $ty {
            fn type_name() -> String {
                $go.to_string()
            }

            fn unmarshal(
                &mut self,
                d: &mut $crate::config::yaml::Decoder,
                n: &$crate::config::yaml::Node,
            ) -> bool {
                d.mapping_struct(self, n)
            }
        }

        impl $crate::config::yaml::Struct for $ty {
            fn unmarshal_field(
                &mut self,
                d: &mut $crate::config::yaml::Decoder,
                name: &str,
                value: &$crate::config::yaml::Node,
            ) -> bool {
                let Self { $($field),* } = self;
                $( $crate::config::yaml::yaml_struct!(@unmarshal d name value $field $name); )*
                let _ = (d, name, value);
                false
            }
        }

        #[cfg(test)]
        impl $crate::config::yaml::Dump for $ty {
            fn dump(&self, path: &str, out: &mut Vec<String>) {
                let Self { $($field),* } = self;
                $( $crate::config::yaml::yaml_struct!(@dump path out $field $name); )*
                let _ = (path, out);
            }
        }
    };
    (@unmarshal $d:ident $n:ident $v:ident $field:ident inline) => {
        if $crate::config::yaml::Struct::unmarshal_field($field, $d, $n, $v) {
            return true;
        }
    };
    (@unmarshal $d:ident $n:ident $v:ident $field:ident skip) => {
        let _ = $field;
    };
    (@unmarshal $d:ident $n:ident $v:ident $field:ident $name:literal) => {
        if $n == $name {
            $crate::config::yaml::Yaml::unmarshal($field, $d, $v);
            return true;
        }
    };
    (@dump $path:ident $out:ident $field:ident inline) => {
        $crate::config::yaml::Dump::dump($field, $path, $out);
    };
    (@dump $path:ident $out:ident $field:ident skip) => {
        let _ = $field;
    };
    (@dump $path:ident $out:ident $field:ident $name:literal) => {
        $crate::config::yaml::Dump::dump($field, &$crate::config::yaml::join($path, $name), $out);
    };
}
pub(crate) use yaml_struct;

// ----------------------------------------------------------------------------
// For the tests that hold the port against the original: every value of a configuration
// as a line, `path=value`, which a Go test prints the same way.

#[cfg(test)]
pub trait Dump {
    fn dump(&self, path: &str, out: &mut Vec<String>);
}

#[cfg(test)]
pub fn join(path: &str, name: &str) -> String {
    if path.is_empty() {
        return name.to_string();
    }
    format!("{path}.{name}")
}

#[cfg(test)]
mod dump {
    use super::*;

    macro_rules! dump_display {
        ($($ty:ty),*) => {$(
            impl Dump for $ty {
                fn dump(&self, path: &str, out: &mut Vec<String>) {
                    out.push(format!("{path}={self}"));
                }
            }
        )*};
    }

    dump_display!(bool, i32, i64, u8, u16, u32, u64, usize, f32, f64);

    impl Dump for String {
        fn dump(&self, path: &str, out: &mut Vec<String>) {
            out.push(format!("{path}={self:?}"));
        }
    }

    impl Dump for Duration {
        fn dump(&self, path: &str, out: &mut Vec<String>) {
            out.push(format!("{path}={}", self.nanoseconds()));
        }
    }

    impl<T: Dump> Dump for Vec<T> {
        fn dump(&self, path: &str, out: &mut Vec<String>) {
            out.push(format!("{path}.len={}", self.len()));
            for (i, e) in self.iter().enumerate() {
                e.dump(&format!("{path}[{i}]"), out);
            }
        }
    }

    impl<K: ToString, V: Dump> Dump for HashMap<K, V> {
        fn dump(&self, path: &str, out: &mut Vec<String>) {
            out.push(format!("{path}.len={}", self.len()));
            let mut entries: Vec<(String, &V)> =
                self.iter().map(|(k, v)| (k.to_string(), v)).collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            for (k, v) in entries {
                v.dump(&format!("{path}{{{k}}}"), out);
            }
        }
    }

    impl<T: Dump> Dump for Option<T> {
        fn dump(&self, path: &str, out: &mut Vec<String>) {
            match self {
                Some(v) => v.dump(path, out),
                None => out.push(format!("{path}=nil")),
            }
        }
    }

    // not held against the original
    impl Dump for IgnoredSection {
        fn dump(&self, _path: &str, _out: &mut Vec<String>) {}
    }
}

#[cfg(test)]
#[path = "yaml_test.rs"]
mod yaml_test;
