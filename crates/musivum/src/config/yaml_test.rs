//! Tests of the reading of YAML, new for musivum. What it gives for whole configurations is
//! held against the original in `config_test.rs`; these are for the parts.

use std::collections::HashMap;

use super::*;
use crate::mediatransportutil::rtcconfig;

#[derive(Clone, Debug, Default, PartialEq)]
struct Inner {
    a: i64,
    b: String,
}

yaml_struct!(Inner, "test.Inner", { a: "a", b: "b" });

#[derive(Clone, Debug, Default, PartialEq)]
struct Outer {
    inner: Inner,
    list: Vec<Inner>,
    map: HashMap<String, Inner>,
    pointer: Option<i64>,
    embedded: Inner,
    port: rtcconfig::PortRange,
    not_in_yaml: bool,
}

yaml_struct!(Outer, "test.Outer", {
    inner: "inner",
    list: "list",
    map: "map",
    pointer: "pointer",
    embedded: inline,
    port: "port",
    not_in_yaml: skip,
});

fn outer() -> Outer {
    Outer {
        inner: Inner {
            a: 1,
            b: "one".to_string(),
        },
        list: vec![Inner {
            a: 2,
            b: "two".to_string(),
        }],
        map: HashMap::from([(
            "k".to_string(),
            Inner {
                a: 3,
                b: "three".to_string(),
            },
        )]),
        pointer: Some(4),
        embedded: Inner {
            a: 5,
            b: "five".to_string(),
        },
        port: rtcconfig::PortRange { start: 6, end: 7 },
        not_in_yaml: true,
    }
}

#[test]
fn a_scalar_is_what_yaml_v3_resolves_it_to() {
    let cases: &[(&str, Resolved)] = &[
        ("", Resolved::Null),
        ("~", Resolved::Null),
        ("null", Resolved::Null),
        ("NULL", Resolved::Null),
        ("nULL", Resolved::Str),
        ("true", Resolved::Bool(true)),
        ("False", Resolved::Bool(false)),
        ("yes", Resolved::Str),
        ("on", Resolved::Str),
        ("0", Resolved::Int(0)),
        ("-12", Resolved::Int(-12)),
        ("+12", Resolved::Int(12)),
        ("1_000", Resolved::Int(1000)),
        ("0x1F", Resolved::Int(31)),
        ("-0x1f", Resolved::Int(-31)),
        ("0b101", Resolved::Int(5)),
        ("0o17", Resolved::Int(15)),
        ("017", Resolved::Int(15)),
        ("08", Resolved::Float(8.0)),
        ("9223372036854775807", Resolved::Int(i64::MAX)),
        ("-9223372036854775808", Resolved::Int(i64::MIN)),
        ("9223372036854775808", Resolved::Uint(1 << 63)),
        (
            "18446744073709551616",
            Resolved::Float(18446744073709551616.0),
        ),
        (
            "-9223372036854775809",
            Resolved::Float(-9223372036854775809.0),
        ),
        ("1.5", Resolved::Float(1.5)),
        (".5", Resolved::Float(0.5)),
        ("5.", Resolved::Float(5.0)),
        ("-1e3", Resolved::Float(-1000.0)),
        ("1e999", Resolved::Str),
        (".inf", Resolved::Float(f64::INFINITY)),
        ("-.INF", Resolved::Float(f64::NEG_INFINITY)),
        ("+inf", Resolved::Str),
        ("nan", Resolved::Str),
        ("0x", Resolved::Str),
        ("1.2.3", Resolved::Str),
        ("5s", Resolved::Str),
        ("-", Resolved::Str),
        ("192.0.2.1", Resolved::Str),
        ("<<", Resolved::Str),
        ("text", Resolved::Str),
    ];
    for (text, want) in cases {
        assert_eq!(*want, resolve(text), "{text:?}");
    }
    assert!(matches!(resolve(".nan"), Resolved::Float(f) if f.is_nan()));
}

#[test]
fn a_document_is_laid_over_the_value_that_is_there() {
    // nothing named: nothing changes
    let mut out = outer();
    decode("{}", true, &mut out).unwrap();
    assert_eq!(outer(), out);

    let text = "
inner:
  a: 10
list:
  - b: new
map:
  k:
    a: 30
  l:
    b: more
a: 50
port: 60
";
    let mut out = outer();
    decode(text, true, &mut out).unwrap();
    // a struct keeps the fields that are not named
    assert_eq!("one", out.inner.b);
    assert_eq!(10, out.inner.a);
    // a slice is a new one, of new values
    assert_eq!(
        vec![Inner {
            a: 0,
            b: "new".to_string()
        }],
        out.list
    );
    // a map keeps its other keys, and a key that is named gets a new value
    assert_eq!(2, out.map.len());
    assert_eq!(
        Inner {
            a: 30,
            b: String::new()
        },
        out.map["k"]
    );
    assert_eq!("more", out.map["l"].b);
    // a struct that is inline has its fields among those of the struct it is in
    assert_eq!(
        Inner {
            a: 50,
            b: "five".to_string()
        },
        out.embedded
    );
    // a type that reads itself (`UnmarshalYAML`)
    assert_eq!(rtcconfig::PortRange { start: 60, end: 7 }, out.port);
    assert_eq!(Some(4), out.pointer);
    assert!(out.not_in_yaml);
}

#[test]
fn a_null_empties_what_can_be_nil_and_leaves_the_rest() {
    let text = "
inner: ~
list:
map: null
pointer:
a:
port: ~
";
    let mut out = outer();
    decode(text, true, &mut out).unwrap();
    let mut want = outer();
    want.list = Vec::new();
    want.map = HashMap::new();
    want.pointer = None;
    assert_eq!(want, out);
}

#[test]
fn an_alias_is_what_it_names_and_a_merge_key_is_laid_under() {
    let text = "
inner: &base
  a: 10
  b: base
list:
  - *base
  - <<: *base
    b: own
  - <<: [{a: 1}, {a: 2, b: second}]
map:
  k: *base
";
    let mut out = outer();
    decode(text, true, &mut out).unwrap();
    let base = Inner {
        a: 10,
        b: "base".to_string(),
    };
    assert_eq!(base, out.inner);
    assert_eq!(
        vec![
            base.clone(),
            Inner {
                a: 10,
                b: "own".to_string()
            },
            Inner {
                a: 1,
                b: "second".to_string()
            },
        ],
        out.list
    );
    assert_eq!(base, out.map["k"]);

    let err = decode("inner:\n  <<: 5", true, &mut outer()).unwrap_err();
    assert_eq!(
        "yaml: map merge requires map or sequence of maps as the value",
        err.to_string()
    );
}

#[test]
fn aliases_cannot_make_a_document_without_end() {
    let mut text = String::from("a0: &a0 [x, x, x, x, x, x, x, x, x, x]\n");
    for i in 1..8 {
        let p = i - 1;
        text.push_str(&format!(
            "a{i}: &a{i} [*a{p}, *a{p}, *a{p}, *a{p}, *a{p}, *a{p}, *a{p}, *a{p}, *a{p}, *a{p}]\n"
        ));
    }
    let err = parse(&text).unwrap_err();
    assert_eq!(
        "yaml: document contains excessive aliasing",
        err.to_string()
    );
}

#[test]
fn a_key_that_is_not_known_is_an_error_only_when_fields_have_to_be_known() {
    let text = "inner:\n  a: 1\n  c: 2\nd: 3\n";
    let mut out = outer();
    decode(text, false, &mut out).unwrap();
    assert_eq!(1, out.inner.a);

    let err = decode(text, true, &mut outer()).unwrap_err();
    assert_eq!(
        "yaml: unmarshal errors:\n  line 3: field c not found in type test.Inner\n  line 4: field d not found in type test.Outer",
        err.to_string()
    );
}

#[test]
fn the_error_of_a_type_that_reads_itself_stops_the_decoding() {
    let err = decode("inner: [1]\nport: 9-8\nd: 3\n", true, &mut outer()).unwrap_err();
    assert_eq!(
        "end port 8 must be greater than start port 9",
        err.to_string()
    );
}

#[test]
fn a_tag_that_contradicts_its_scalar_stops_the_decoding() {
    let err = decode("inner:\n  a: !!int five\n", true, &mut outer()).unwrap_err();
    assert_eq!(
        "yaml: cannot decode !!str `five` as a !!int",
        err.to_string()
    );

    let mut out = outer();
    decode(
        "inner:\n  a: !!int '7'\n  b: !!binary aGVsbG8=\n",
        true,
        &mut out,
    )
    .unwrap();
    assert_eq!(7, out.inner.a);
    assert_eq!("hello", out.inner.b);
}

#[test]
fn text_without_a_document_is_the_end_for_a_decoder_and_nothing_for_unmarshal() {
    for text in ["", "\n", "# only a comment\n"] {
        assert_eq!(
            Err(Error::Eof),
            decode(text, true, &mut outer()),
            "{text:?}"
        );
        let mut out = outer();
        unmarshal(text, &mut out).unwrap();
        assert_eq!(outer(), out);
    }
    // a document that is empty is a null
    let mut out = outer();
    decode("---\n", true, &mut out).unwrap();
    assert_eq!(outer(), out);
}

#[test]
fn what_the_parser_refuses_is_an_error_with_its_line() {
    let err = decode("inner:\n  a: [1\nlist: x\n", true, &mut outer()).unwrap_err();
    let text = err.to_string();
    assert!(text.starts_with("yaml: line "), "{text}");
}
