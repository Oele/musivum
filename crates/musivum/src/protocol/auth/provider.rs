// Ported from LiveKit's protocol, auth/provider.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The YAML decoder of the original (yaml.v3, decoding
// into a map of strings) is the function `decode` here, written on the events of the crate
// `saphyr-parser`. It reads a key file of `key: secret` lines, and the other ways YAML has
// to write a mapping of strings, as that decoder does. Where the two differ (the tests have
// each): merge keys (`<<`) and aliases to anything but a string are errors here; a tab right
// after the colon is an error here and taken by the original; a tab at the start of a line,
// a `%YAML 1.2` directive and a key that is left out (`: secret`) are taken here and errors
// to the original.

use std::collections::HashMap;
use std::io::Read;

use base64::Engine as _;
use saphyr_parser::{Event, Parser, ScalarStyle, Tag};

use super::interfaces::{Error, KeyProvider};

pub struct FileBasedKeyProvider {
    keys: HashMap<String, String>,
}

impl FileBasedKeyProvider {
    pub fn new_from_reader(mut r: impl Read) -> Result<FileBasedKeyProvider, Error> {
        let mut text = String::new();
        r.read_to_string(&mut text)
            .map_err(|err| Error::Yaml(format!("input error: {err}")))?;
        let keys = decode(&text)?;
        Ok(FileBasedKeyProvider { keys })
    }

    pub fn new_from_map(keys: HashMap<String, String>) -> FileBasedKeyProvider {
        FileBasedKeyProvider { keys }
    }
}

impl KeyProvider for FileBasedKeyProvider {
    fn get_secret(&self, key: &str) -> String {
        self.keys.get(key).cloned().unwrap_or_default()
    }

    fn num_keys(&self) -> usize {
        self.keys.len()
    }
}

pub struct SimpleKeyProvider {
    api_key: String,
    api_secret: String,
}

impl SimpleKeyProvider {
    pub fn new(api_key: &str, api_secret: &str) -> SimpleKeyProvider {
        SimpleKeyProvider {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
        }
    }
}

impl KeyProvider for SimpleKeyProvider {
    fn get_secret(&self, key: &str) -> String {
        if key == self.api_key {
            return self.api_secret.clone();
        }
        String::new()
    }

    fn num_keys(&self) -> usize {
        1
    }
}

// What `yaml.NewDecoder(r).Decode(&keys)` does for a map of strings: it reads the first
// document, which has to be a mapping of one scalar to another, and takes each scalar as
// the text that is written, whatever YAML would make of it (a number, true). A value that
// is left out or null is the empty string; a key that is null is left out with its value. A
// key that is there twice is an error.
fn decode(text: &str) -> Result<HashMap<String, String>, Error> {
    let mut keys = HashMap::new();
    // The scalars that were given a name (`&name`), for the aliases to them (`*name`).
    let mut anchors: HashMap<usize, Option<String>> = HashMap::new();

    let mut parser = Parser::new_from_str(text);
    let mut next = || match parser.next() {
        Some(Ok((event, _))) => Ok(event),
        Some(Err(err)) => Err(Error::Yaml(err.to_string())),
        None => Ok(Event::StreamEnd),
    };

    loop {
        match next()? {
            Event::DocumentStart(_) => break,
            // Nothing but comments, or nothing at all.
            Event::StreamEnd => return Err(Error::Eof),
            _ => {}
        }
    }

    match next()? {
        Event::MappingStart(..) => {}
        // A document that is null gives no keys.
        Event::Scalar(value, style, _, tag) if is_null(&value, style, tag.as_deref()) => {
            return Ok(keys);
        }
        Event::SequenceStart(..) => return Err(cannot_unmarshal("!!seq", "map[string]string")),
        _ => return Err(cannot_unmarshal("!!str", "map[string]string")),
    }

    loop {
        let key = match next()? {
            Event::MappingEnd => break,
            event => scalar(event, &mut anchors)?,
        };
        let Some(key) = key else {
            // A key that is null: the entry is left out, whatever its value is.
            let mut depth = 0usize;
            loop {
                match next()? {
                    Event::SequenceStart(..) | Event::MappingStart(..) => depth += 1,
                    Event::SequenceEnd | Event::MappingEnd => depth = depth.saturating_sub(1),
                    Event::StreamEnd => break,
                    _ => {}
                }
                if depth == 0 {
                    break;
                }
            }
            continue;
        };
        let value = scalar(next()?, &mut anchors)?;
        if keys.contains_key(&key) {
            return Err(Error::Yaml(format!(
                "unmarshal errors:\n  mapping key {key:?} already defined"
            )));
        }
        keys.insert(key, value.unwrap_or_default());
    }
    Ok(keys)
}

fn cannot_unmarshal(what: &str, into: &str) -> Error {
    Error::Yaml(format!(
        "unmarshal errors:\n  cannot unmarshal {what} into {into}"
    ))
}

// Whether a scalar is YAML's null: left out, or written `~` or `null`, and not in quotes.
fn is_null(value: &str, style: ScalarStyle, tag: Option<&Tag>) -> bool {
    style == ScalarStyle::Plain
        && tag.is_none()
        && matches!(value, "" | "~" | "null" | "Null" | "NULL")
}

// The text of a key or of a value; `None` for one that is null.
fn scalar(
    event: Event<'_>,
    anchors: &mut HashMap<usize, Option<String>>,
) -> Result<Option<String>, Error> {
    match event {
        Event::Scalar(value, style, anchor_id, tag) => {
            let tag = tag.as_deref();
            let text = if is_null(&value, style, tag) {
                None
            } else if tag.is_some_and(|tag| tag.is_yaml_core_schema() && tag.suffix == "binary") {
                // Go's decoder of base64 skips line breaks.
                let data = base64::engine::general_purpose::STANDARD
                    .decode(value.replace(['\r', '\n'], ""))
                    .map_err(|_| {
                        Error::Yaml("!!binary value contains invalid base64 data".into())
                    })?;
                let text = String::from_utf8(data)
                    .map_err(|_| Error::Yaml("!!binary value is not text".into()))?;
                Some(text)
            } else {
                Some(value.into_owned())
            };
            if anchor_id > 0 {
                anchors.insert(anchor_id, text.clone());
            }
            Ok(text)
        }
        Event::Alias(anchor_id) => match anchors.get(&anchor_id) {
            Some(text) => Ok(text.clone()),
            None => Err(cannot_unmarshal(
                "an alias to what is not a string",
                "string",
            )),
        },
        Event::SequenceStart(..) => Err(cannot_unmarshal("!!seq", "string")),
        Event::MappingStart(..) => Err(cannot_unmarshal("!!map", "string")),
        // The parser gives a key and a value for each entry of a mapping.
        _ => Err(Error::Yaml(
            "mapping values are not allowed in this context".into(),
        )),
    }
}

#[cfg(test)]
#[path = "provider_test.rs"]
mod provider_test;
