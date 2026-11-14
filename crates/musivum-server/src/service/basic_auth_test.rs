// New for musivum: upstream has no test of basic_auth.go. What `basic_auth` reads from a
// request, as Go's `r.BasicAuth()` reads it; the middleware is tested over a listener in
// promserver_test.rs.

use axum::http::HeaderValue;

use super::*;

fn auth(value: &str) -> Option<(String, String)> {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_str(value).unwrap());
    basic_auth(&headers)
        .map(|(u, p)| (String::from_utf8(u).unwrap(), String::from_utf8(p).unwrap()))
}

#[test]
fn test_basic_auth() {
    assert_eq!(None, basic_auth(&HeaderMap::new()));

    let some = |u: &str, p: &str| Some((u.to_string(), p.to_string()));
    for (value, want) in [
        // "Aladdin:open sesame", RFC 7617's example
        (
            "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==",
            some("Aladdin", "open sesame"),
        ),
        // the scheme in any case
        (
            "basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==",
            some("Aladdin", "open sesame"),
        ),
        (
            "BASIC QWxhZGRpbjpvcGVuIHNlc2FtZQ==",
            some("Aladdin", "open sesame"),
        ),
        // cut at the first colon: "a:b:c"
        ("Basic YTpiOmM=", some("a", "b:c")),
        // ":" is an empty user name and password
        ("Basic Og==", some("", "")),
        // no colon: "abc"
        ("Basic YWJj", None),
        // another scheme, or none
        ("Bearer YTpi", None),
        ("Basic", None),
        ("", None),
        // two spaces: the second is not base64
        ("Basic  YTpi", None),
        // padding missing, misplaced or too long
        ("Basic YTpiYw", None),
        ("Basic YTpiYw=", None),
        ("Basic YT=iYw==", None),
        ("Basic YTpi====", None),
        // bits left over need not be zero, as in Go's decoder: "a:bc" with the last bits set
        ("Basic YTpiYw==", some("a", "bc")),
        ("Basic YTpiYx==", some("a", "bc")),
        // not the alphabet
        ("Basic YTpi!w==", None),
        ("Basic YTpi-w==", None),
    ] {
        assert_eq!(want, auth(value), "{value:?}");
    }

    // carriage returns and newlines are passed over (a header value cannot hold them: the
    // decoder is asked directly)
    assert_eq!(Some(b"a:b".to_vec()), decode_std_base64(b"YT\npi\r"));
    assert_eq!(Some(b"a:bc".to_vec()), decode_std_base64(b"YTpi\r\nYw==\n"));
}
