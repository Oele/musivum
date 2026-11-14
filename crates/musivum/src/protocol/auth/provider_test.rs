// Ported from LiveKit's protocol, auth/provider_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The key file of the test is written next to the test
// binary (in `target/`), not in the system's directory for temporary files. The original has
// one test; the others here are new and say so.

use std::fs::{self, File};
use std::path::PathBuf;

use super::*;

// The original's `defer os.Remove(f.Name())`.
struct RemoveOnDrop(PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[test]
fn test_file_based_key_provider() {
    let keys = HashMap::from([
        ("key1", "secret1"),
        ("key2", "secret2"),
        ("key3", "secret3"),
    ]);
    let dir = std::env::current_exe().unwrap();
    let name = dir
        .parent()
        .unwrap()
        .join(format!("keyfile-{}", std::process::id()));
    let _remove = RemoveOnDrop(name.clone());

    let mut text = String::new();
    text.push_str("key1: secret1\n");
    text.push_str("key2: secret2 \r\n");
    text.push('\n');
    text.push_str("key3: secret3");
    fs::write(&name, text).unwrap();

    let r = File::open(&name).unwrap();
    let p = FileBasedKeyProvider::new_from_reader(r).unwrap();

    for (key, val) in keys {
        assert_eq!(val, p.get_secret(key));
    }
}

fn keys_of(text: &str) -> Result<Vec<(String, String)>, Error> {
    let p = FileBasedKeyProvider::new_from_reader(text.as_bytes())?;
    let mut keys: Vec<(String, String)> = p.keys.into_iter().collect();
    keys.sort();
    Ok(keys)
}

fn pairs(keys: &[(&str, &str)]) -> Result<Vec<(String, String)>, Error> {
    Ok(keys
        .iter()
        .map(|(key, secret)| (key.to_string(), secret.to_string()))
        .collect())
}

// New for musivum: key files as the YAML decoder of the original reads them. The expected
// keys are what the Go original gave for the same text (sorted by key here).
#[test]
fn a_key_file_is_read_as_the_go_implementation_reads_it() {
    // The file of the original's test.
    assert_eq!(
        pairs(&[
            ("key1", "secret1"),
            ("key2", "secret2"),
            ("key3", "secret3")
        ]),
        keys_of("key1: secret1\nkey2: secret2 \r\n\nkey3: secret3")
    );
    // A document that is null has no keys.
    assert_eq!(pairs(&[]), keys_of("~\n"));
    // A secret that is left out or null is the empty string.
    assert_eq!(
        pairs(&[("key1", ""), ("key2", ""), ("key3", ""), ("key4", "")]),
        keys_of("key1:\nkey2: ~\nkey3: null\nkey4: ''\n")
    );
    // What YAML would read as a number or as true is the text that is written.
    assert_eq!(
        pairs(&[
            ("123", "abc"),
            ("key1", "12345"),
            ("key2", "0012"),
            ("key3", "true"),
            ("key4", "1.50"),
            ("key5", "0x1F"),
            ("true", "yes"),
        ]),
        keys_of(
            "key1: 12345\nkey2: 0012\nkey3: true\nkey4: 1.50\nkey5: 0x1F\n123: abc\ntrue: yes\n"
        )
    );
    // Quotes and comments.
    assert_eq!(
        pairs(&[
            ("key1", "quoted # not a comment"),
            ("key2", "double \"q\" \n"),
            ("key3", "plain"),
        ]),
        keys_of(
            "key1: 'quoted # not a comment'\nkey2: \"double \\\"q\\\" \\n\"\nkey3: plain # a comment\n"
        )
    );
    // Only the first document is read.
    assert_eq!(
        pairs(&[("key1", "secret1")]),
        keys_of("key1: secret1\n---\nkey2: secret2\n")
    );
    assert_eq!(
        pairs(&[("key1", "secret1")]),
        keys_of("---\nkey1: secret1\n")
    );
    // The other ways to write a mapping and a string.
    assert_eq!(
        pairs(&[("key1", "secret1"), ("key2", "secret2")]),
        keys_of("{key1: secret1, key2: secret2}\n")
    );
    assert_eq!(
        pairs(&[("key1", "line1\nline2\n"), ("key2", "folded text\n")]),
        keys_of("key1: |\n  line1\n  line2\nkey2: >\n  folded\n  text\n")
    );
    assert_eq!(
        pairs(&[("key1", "secret1"), ("key2", "secret1")]),
        keys_of("key1: &a secret1\nkey2: *a\n")
    );
    assert_eq!(
        pairs(&[("key1", "123"), ("key2", "secret")]),
        keys_of("key1: !!str 123\nkey2: !!binary c2VjcmV0\n")
    );
    assert_eq!(
        pairs(&[
            ("key 3", "v"),
            ("key1", "secret with spaces"),
            ("key2", "lead"),
        ]),
        keys_of("key1: secret with spaces  \nkey2:   lead\n\"key 3\": v\n")
    );

    for (text, want) in [
        // Tabs, where they do not indent.
        ("key1: \tsecret1\n", &[("key1", "secret1")][..]),
        ("key1: secret1\t\n", &[("key1", "secret1")]),
        ("key1: sec\tret\n", &[("key1", "sec\tret")]),
        // A key that is null is left out, with whatever it is the key of. A key that is
        // the empty string is a key.
        ("~: x\n", &[]),
        ("null: x\n", &[]),
        ("~: x\nkey1: secret1\n", &[("key1", "secret1")]),
        ("~: [a]\nkey1: secret1\n", &[("key1", "secret1")]),
        ("'': x\n", &[("", "x")]),
        // Whatever the type YAML or a tag gives a scalar, it is the text that is written.
        ("key1: 2001-12-14\n", &[("key1", "2001-12-14")]),
        ("key1: !!int 5\n", &[("key1", "5")]),
        ("key1: !custom 5\n", &[("key1", "5")]),
        (
            "key1: 1_000\nkey2: 0o17\nkey3: .inf\nkey4: yes\nkey5: 1e3\n",
            &[
                ("key1", "1_000"),
                ("key2", "0o17"),
                ("key3", ".inf"),
                ("key4", "yes"),
                ("key5", "1e3"),
            ],
        ),
        // The three spellings of null, and one that is not.
        (
            "key1: Null\nkey2: NULL\nkey3: nUll\n",
            &[("key1", ""), ("key2", ""), ("key3", "nUll")],
        ),
        (
            "key1: 'it''s'\nkey2: \"a\\tb\\u00e9\"\n",
            &[("key1", "it's"), ("key2", "a\tb\u{e9}")],
        ),
        (
            "\"key1\": secret1\n'key2': secret2\n",
            &[("key1", "secret1"), ("key2", "secret2")],
        ),
        (
            "key1: secret1 # comment\nkey2: secret#2\n",
            &[("key1", "secret1"), ("key2", "secret#2")],
        ),
        (
            "key1: se:cret\nkey2: a-b_c+d/e=\n",
            &[("key1", "se:cret"), ("key2", "a-b_c+d/e=")],
        ),
        (
            "key1: secret1\n...\nkey2: secret2\n",
            &[("key1", "secret1")],
        ),
        ("? key1\n: secret1\n", &[("key1", "secret1")]),
        (
            "key1: secret1\r\nkey2: secret2\r\n",
            &[("key1", "secret1"), ("key2", "secret2")],
        ),
        ("\u{feff}key1: secret1\n", &[("key1", "secret1")]),
        ("\n\n# c\n\nkey1: secret1\n", &[("key1", "secret1")]),
        (
            "  key1: secret1\n  key2: secret2\n",
            &[("key1", "secret1"), ("key2", "secret2")],
        ),
        ("{}\n", &[]),
        ("null\n", &[]),
    ] {
        assert_eq!(pairs(want), keys_of(text), "{text:?}");
    }
}

// New for musivum: the key files the YAML decoder of the original refuses. The texts of the
// errors are the original's as far as they are given here.
#[test]
fn a_key_file_that_the_go_implementation_refuses() {
    // No document: the original returns io.EOF.
    assert_eq!(Err(Error::Eof), keys_of(""));
    assert_eq!(Err(Error::Eof), keys_of("# only a comment\n"));
    assert_eq!("EOF", Error::Eof.to_string());

    let unmarshal = |text: &str| match keys_of(text) {
        Err(Error::Yaml(msg)) => msg,
        other => panic!("{text:?}: {other:?}"),
    };
    assert_eq!(
        "unmarshal errors:\n  mapping key \"key1\" already defined",
        unmarshal("key1: secret1\nkey1: secret2\n")
    );
    assert_eq!(
        "unmarshal errors:\n  cannot unmarshal !!seq into string",
        unmarshal("key1: [a, b]\n")
    );
    assert_eq!(
        "unmarshal errors:\n  cannot unmarshal !!map into string",
        unmarshal("key1: {a: b}\nkey2: ok\n")
    );
    assert_eq!(
        "unmarshal errors:\n  cannot unmarshal !!seq into map[string]string",
        unmarshal("- a\n- b\n")
    );
    assert_eq!(
        "unmarshal errors:\n  cannot unmarshal !!str into map[string]string",
        unmarshal("just a string\n")
    );
    assert_eq!(
        "yaml: unmarshal errors:\n  cannot unmarshal !!seq into string",
        keys_of("key1: [a, b]\n").unwrap_err().to_string()
    );
    // What is not YAML, and what is not a mapping of strings: the words for it are the
    // parser's own, or stand in for the original's.
    for text in [
        "key1: secret1\n  bad indent: x\n",
        "key1: a: b\n",
        "key1: secret1\n key2: secret2\n",
        "key1: [\n",
        "key1: 'unterminated\n",
        "key1: @at\n",
        "key1: `tick\n",
        "key1: -dash\nkey2: - dash\n",
        "key1: *unknown\n",
        "base: &b {a: b}\n",
        "key1: {}\n",
        "key1: []\n",
        "[a, b]: c\n",
        "[]\n",
        "''\n",
        "5\n",
    ] {
        assert!(
            matches!(keys_of(text), Err(Error::Yaml(_))),
            "{text:?}: {:?}",
            keys_of(text)
        );
    }
}

// New for musivum: where a key file is not read as the original reads it.
#[test]
fn a_key_file_that_is_not_read_as_the_go_implementation_reads_it() {
    // A tab at the start of a line is refused by the original ("found character that cannot
    // start any token", "found a tab character that violates indentation").
    assert_eq!(pairs(&[("key1", "secret1")]), keys_of("\tkey1: secret1\n"));
    // And a tab right after the colon, with no space before the secret, is taken by the
    // original ({"key1":"secret1"}) and refused by the YAML parser used here.
    assert!(matches!(keys_of("key1:\tsecret1\n"), Err(Error::Yaml(_))));
    assert!(matches!(keys_of("key1:\t5\n"), Err(Error::Yaml(_))));
    // The original knows YAML 1.1 only ("found incompatible YAML document").
    assert_eq!(
        pairs(&[("key1", "secret1")]),
        keys_of("%YAML 1.2\n---\nkey1: secret1\n")
    );
    // The original merges the mapping into the keys ({"a":"b","key1":"c"}).
    assert!(matches!(
        keys_of("<<: {a: b}\nkey1: c\n"),
        Err(Error::Yaml(_))
    ));
    // A key that is left out is refused by the original ("did not find expected key"); here
    // it is a key that is null.
    assert_eq!(pairs(&[]), keys_of(": x\n"));
}

// New for musivum: the providers that are not read from a file, which the original does not
// test.
#[test]
fn the_providers_give_the_secret_of_a_key() {
    let p = FileBasedKeyProvider::new_from_map(HashMap::from([
        ("key1".to_string(), "secret1".to_string()),
        ("key2".to_string(), "secret2".to_string()),
    ]));
    assert_eq!("secret1", p.get_secret("key1"));
    assert_eq!("secret2", p.get_secret("key2"));
    // A key that is not known has no secret.
    assert_eq!("", p.get_secret("key3"));
    assert_eq!("", p.get_secret(""));
    assert_eq!(2, p.num_keys());

    let p = SimpleKeyProvider::new("key", "secret");
    assert_eq!("secret", p.get_secret("key"));
    assert_eq!("", p.get_secret("another"));
    assert_eq!("", p.get_secret(""));
    assert_eq!(1, p.num_keys());

    // Both are key providers.
    let providers: Vec<std::sync::Arc<dyn KeyProvider>> = vec![
        std::sync::Arc::new(p),
        std::sync::Arc::new(
            FileBasedKeyProvider::new_from_reader("key: secret".as_bytes()).unwrap(),
        ),
    ];
    for p in providers {
        assert_eq!("secret", p.get_secret("key"));
        assert_eq!(1, p.num_keys());
    }
}
