// Ported in part from golang-jwt/jwt, the JWT library the original is built on: parser.go,
// validator.go, types.go, registered_claims.go, token.go, hmac.go and errors.go at v5.3.1.
// Copyright (c) 2012 Dave Grijalva, Copyright (c) 2021 golang-jwt maintainers. Licensed under
// the MIT License.
// Changes: translated from Go to Rust, and only what LiveKit's `auth` package uses of the
// library: tokens signed with HMAC-SHA256, the registered claims, and a parser with the
// options `WithValidMethods`, `WithIssuer`, `WithLeeway` and `WithExpirationRequired`. The
// options are the fields of `Parser`, a key is bytes, the errors are enums with the library's
// texts, and a date is kept as its seconds (the library's `TimePrecision`). The tests are
// new.

//! What the original takes from its JWT library, `github.com/golang-jwt/jwt/v5`: tokens
//! signed with HMAC-SHA256 (RFC 7515, RFC 7519), the registered claims, and the checks that
//! `Verify` asks for, on the crates `hmac`, `sha2`, `base64` and `serde_json`.
//!
//! It keeps to that library's rules wherever a token can tell the difference, so that what
//! LiveKit's server takes and refuses is taken and refused here, and it gives that library's
//! error texts:
//!
//! - A token is three segments in base64 for URLs without padding. Line breaks in a segment
//!   are skipped, as Go's decoder skips them, so a token with a line break after it is good.
//! - The claims are read by Go's rules for JSON ([`super::json`]).
//! - `exp`, `nbf` and `iat` are numbers of seconds, with or without a fraction, or such a
//!   number in quotes; they are kept in whole seconds.
//! - A date is compared as Go's `time.Time` compares it, which counts the seconds from the
//!   year 1 in 64 bits: an `exp` after the last second that holds is a date long ago, and
//!   such a token has expired.
//! - "now" is the clock of the system ([`time_now`]).

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use hmac::{Hmac, KeyInit, Mac};
use serde::de::{DeserializeOwned, Error as _, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use sha2::Sha256;

use super::json;
use crate::go::time::{Duration, Time};

/// `jwt.SigningMethodHS256.Alg()`.
pub const SIGNING_METHOD_HS256: &str = "HS256";

// The signing methods the library knows. A token that names another is refused when it is
// parsed; one that names one of these that is not allowed, when it is verified.
const SIGNING_METHODS: &[&str] = &[
    "HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256",
    "ES384", "ES512", "EdDSA", "none",
];

// Go's `base64.RawURLEncoding`, which is not strict about the bits of the last character
// that are not used.
const RAW_URL_ENCODING: GeneralPurpose = GeneralPurpose::new(
    &alphabet::URL_SAFE,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_padding_mode(DecodePaddingMode::RequireNone)
        .with_decode_allow_trailing_bits(true),
);

/// The errors of the library, with its texts.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// `jwt.ErrTokenMalformed`
    #[error("token is malformed: {0}")]
    TokenMalformed(String),
    /// `jwt.ErrTokenUnverifiable`
    #[error("token is unverifiable: {0}")]
    TokenUnverifiable(String),
    /// `jwt.ErrTokenSignatureInvalid`
    #[error("token signature is invalid: {0}")]
    TokenSignatureInvalid(String),
    /// `jwt.ErrTokenInvalidClaims`, with everything that is wrong with the claims.
    #[error("token has invalid claims: {}", join(.0))]
    TokenInvalidClaims(Vec<ClaimsError>),
}

/// What can be wrong with the claims of a token whose signature is good.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClaimsError {
    /// `jwt.ErrTokenExpired`
    #[error("token is expired")]
    TokenExpired,
    /// `jwt.ErrTokenNotValidYet`
    #[error("token is not valid yet")]
    TokenNotValidYet,
    /// `jwt.ErrTokenRequiredClaimMissing`, with the claim.
    #[error("token is missing required claim: {0} claim is required")]
    TokenRequiredClaimMissing(&'static str),
    /// `jwt.ErrTokenInvalidIssuer`
    #[error("token has invalid issuer")]
    TokenInvalidIssuer,
}

fn join(errs: &[ClaimsError]) -> String {
    let msg: Vec<String> = errs.iter().map(ClaimsError::to_string).collect();
    msg.join(", ")
}

/// Go's `time.Now()` for the times of a token: the clock of the system.
///
/// Not [`crate::go::time::now`], which reads the system's clock once, when the process
/// starts, and counts on from there by a clock that is never set. The times in a token are
/// compared with the clocks of other machines; a server that started before its clock was
/// set right, or slept, would refuse every token from then on.
pub fn time_now() -> Time {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Time::from_unix_nano(since_epoch.as_nanos() as i64)
}

/// `jwt.NumericDate`: a time as a token carries it, in seconds since 1970. The library keeps
/// whole seconds (its `TimePrecision`), and so does this.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NumericDate(pub i64);

impl NumericDate {
    /// `jwt.NewNumericDate(t)`: the time, cut to the second.
    pub fn new(t: Time) -> NumericDate {
        NumericDate(t.unix_secs())
    }

    /// `jwt.NewNumericDate(t.Add(d))`, also when the sum is later than a [`Time`] can be.
    pub fn new_after(t: Time, d: Duration) -> NumericDate {
        let nanos = t.unix_nano() as i128 + d.nanoseconds() as i128;
        NumericDate(nanos.div_euclid(1_000_000_000) as i64)
    }

    /// The seconds since 1970, Go's `date.Unix()`.
    pub fn unix(self) -> i64 {
        self.0
    }

    /// The time. A date that a [`Time`] cannot hold (one after the year 2262) is the last
    /// one it can hold.
    pub fn time(self) -> Time {
        Time::from_unix_nano(self.0.saturating_mul(1_000_000_000))
    }
}

// The library's newNumericDateFromSeconds: the seconds a token gives, cut to the second.
fn new_numeric_date_from_seconds(f: f64) -> NumericDate {
    let sec = f.floor();
    // Go's `int64(f)` for a number that an int64 cannot hold is what the processor makes of
    // it: on amd64, which LiveKit's servers run on, the least int64. (Rust's `as` gives the
    // nearest one. Either is a date long ago to Go, see `before`.)
    if (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&sec) {
        NumericDate(sec as i64)
    } else {
        NumericDate(i64::MIN)
    }
}

impl Serialize for NumericDate {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(self.0)
    }
}

impl<'de> Deserialize<'de> for NumericDate {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<NumericDate, D::Error> {
        struct NumericDateVisitor;

        impl Visitor<'_> for NumericDateVisitor {
            type Value = NumericDate;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a number of seconds")
            }

            // The library reads every number as a float.
            fn visit_i64<E>(self, v: i64) -> Result<NumericDate, E> {
                Ok(new_numeric_date_from_seconds(v as f64))
            }

            fn visit_u64<E>(self, v: u64) -> Result<NumericDate, E> {
                Ok(new_numeric_date_from_seconds(v as f64))
            }

            fn visit_f64<E>(self, v: f64) -> Result<NumericDate, E> {
                Ok(new_numeric_date_from_seconds(v))
            }

            // The library reads the claim as a `json.Number`, and for that Go also takes a
            // number in quotes: a number as JSON writes it, with nothing around it.
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<NumericDate, E> {
                let number = if v.bytes().any(|b| b.is_ascii_whitespace()) {
                    None
                } else {
                    serde_json::from_str::<f64>(v).ok()
                };
                match number {
                    Some(f) => Ok(new_numeric_date_from_seconds(f)),
                    None => Err(E::custom(format_args!(
                        "could not parse NumericData: invalid number literal {v:?}"
                    ))),
                }
            }
        }

        deserializer.deserialize_any(NumericDateVisitor)
    }
}

/// `jwt.ClaimStrings`: the audience, which a token gives as one string or as a list.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClaimStrings(pub Vec<String>);

impl ClaimStrings {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Serialize for ClaimStrings {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ClaimStrings {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<ClaimStrings, D::Error> {
        // jwt.ErrInvalidType
        let invalid_type = || D::Error::custom("invalid type for claim");
        match Value::deserialize(deserializer)? {
            Value::String(s) => Ok(ClaimStrings(vec![s])),
            Value::Array(values) => values
                .into_iter()
                .map(|value| match value {
                    Value::String(s) => Ok(s),
                    _ => Err(invalid_type()),
                })
                .collect::<Result<_, _>>()
                .map(ClaimStrings),
            Value::Null => Ok(ClaimStrings::default()),
            _ => Err(invalid_type()),
        }
    }
}

/// `jwt.RegisteredClaims`: the claims every JWT may have (RFC 7519, section 4.1).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RegisteredClaims {
    #[serde(rename = "iss", skip_serializing_if = "String::is_empty")]
    pub issuer: String,
    #[serde(rename = "sub", skip_serializing_if = "String::is_empty")]
    pub subject: String,
    #[serde(rename = "aud", skip_serializing_if = "ClaimStrings::is_empty")]
    pub audience: ClaimStrings,
    #[serde(rename = "exp", skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<NumericDate>,
    #[serde(rename = "nbf", skip_serializing_if = "Option::is_none")]
    pub not_before: Option<NumericDate>,
    #[serde(rename = "iat", skip_serializing_if = "Option::is_none")]
    pub issued_at: Option<NumericDate>,
    #[serde(rename = "jti", skip_serializing_if = "String::is_empty")]
    pub id: String,
}

/// What the checks read from a set of claims: `jwt.Claims`, as far as it is used.
pub trait Claims {
    fn get_expiration_time(&self) -> Option<NumericDate>;
    fn get_not_before(&self) -> Option<NumericDate>;
    fn get_issuer(&self) -> &str;
}

impl Claims for RegisteredClaims {
    fn get_expiration_time(&self) -> Option<NumericDate> {
        self.expires_at
    }

    fn get_not_before(&self) -> Option<NumericDate> {
        self.not_before
    }

    fn get_issuer(&self) -> &str {
        &self.issuer
    }
}

pub(super) fn encode_segment(seg: &[u8]) -> String {
    RAW_URL_ENCODING.encode(seg)
}

pub(super) fn decode_segment(seg: &str) -> Result<Vec<u8>, base64::DecodeError> {
    // Go's decoder skips line breaks.
    if seg.contains(['\r', '\n']) {
        RAW_URL_ENCODING.decode(seg.replace(['\r', '\n'], ""))
    } else {
        RAW_URL_ENCODING.decode(seg)
    }
}

fn hmac_sha256(signing_string: &str, key: &[u8]) -> Hmac<Sha256> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes a key of any length");
    mac.update(signing_string.as_bytes());
    mac
}

/// The signature of the first two segments of a token.
pub(super) fn sign(signing_string: &str, key: &[u8]) -> Vec<u8> {
    hmac_sha256(signing_string, key)
        .finalize()
        .into_bytes()
        .to_vec()
}

/// `jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)`.
pub fn signed_string<C: Serialize>(claims: &C, key: &[u8]) -> Result<String, serde_json::Error> {
    // The library writes the header from a map, which Go writes with its keys in order.
    let header = br#"{"alg":"HS256","typ":"JWT"}"#;
    let claims = serde_json::to_vec(claims)?;
    let signing_string = format!("{}.{}", encode_segment(header), encode_segment(&claims));
    let signature = sign(&signing_string, key);
    Ok(format!("{signing_string}.{}", encode_segment(&signature)))
}

/// A token taken apart: `jwt.Token`, as far as it is used.
pub struct Token<'a, C> {
    /// The second segment of the token, read.
    pub claims: C,
    /// The `alg` of the header.
    pub method: String,
    // The third segment of the token.
    signature: Vec<u8>,
    // The first two segments, as they are in the token: what is signed.
    signing_string: &'a str,
}

/// `Parser.ParseUnverified`: takes a token apart and reads its claims. Neither the signature
/// nor the claims are checked.
pub fn parse_unverified<C: DeserializeOwned>(token_string: &str) -> Result<Token<'_, C>, Error> {
    let parts: Vec<&str> = token_string.splitn(4, '.').collect();
    if parts.len() != 3 {
        return Err(Error::TokenMalformed(
            "token contains an invalid number of segments".into(),
        ));
    }

    // Parse Header
    let header_bytes = decode_segment(parts[0])
        .map_err(|err| Error::TokenMalformed(format!("could not base64 decode header: {err}")))?;
    let header = match json::from_slice::<Value>(&header_bytes) {
        // Go leaves its map as it is for null.
        Ok(header @ (Value::Object(_) | Value::Null)) => header,
        Ok(_) => {
            return Err(Error::TokenMalformed(
                "could not JSON decode header: not an object".into(),
            ));
        }
        Err(err) => {
            return Err(Error::TokenMalformed(format!(
                "could not JSON decode header: {err}"
            )));
        }
    };

    // Parse Claims
    let claim_bytes = decode_segment(parts[1])
        .map_err(|err| Error::TokenMalformed(format!("could not base64 decode claim: {err}")))?;
    let claims = json::from_slice::<C>(&claim_bytes)
        .map_err(|err| Error::TokenMalformed(format!("could not JSON decode claim: {err}")))?;

    // Lookup signature method
    let method = match header.get("alg").and_then(Value::as_str) {
        Some(method) if SIGNING_METHODS.contains(&method) => method.to_string(),
        Some(_) => {
            return Err(Error::TokenUnverifiable(
                "signing method (alg) is unavailable".into(),
            ));
        }
        None => {
            return Err(Error::TokenUnverifiable(
                "signing method (alg) is unspecified".into(),
            ));
        }
    };

    // Parse token signature
    let signature = decode_segment(parts[2]).map_err(|err| {
        Error::TokenMalformed(format!("could not base64 decode signature: {err}"))
    })?;

    Ok(Token {
        claims,
        method,
        signature,
        signing_string: &token_string[..parts[0].len() + 1 + parts[1].len()],
    })
}

/// What is asked of a token when it is verified: the library's `Parser` with the options
/// `WithValidMethods`, `WithIssuer`, `WithLeeway` and `WithExpirationRequired`.
#[derive(Clone, Copy, Debug)]
pub struct Parser<'a> {
    /// Only these methods will be considered valid.
    pub valid_methods: &'a [&'a str],
    /// The issuer the token has to name. Empty: any, and none.
    pub issuer: &'a str,
    /// How far the clocks of who made the token and who reads it may be apart.
    pub leeway: Duration,
    /// Whether a token without `exp`, which never expires, is refused.
    pub expiration_required: bool,
}

impl Parser<'_> {
    /// `jwt.ParseWithClaims`: parses, verifies the signature with the key and validates the
    /// claims.
    pub fn parse_with_claims<C: DeserializeOwned + Claims>(
        &self,
        token_string: &str,
        key: &[u8],
    ) -> Result<C, Error> {
        let token = parse_unverified::<C>(token_string)?;

        // Verify signing method is in the required set
        if !self.valid_methods.contains(&token.method.as_str()) {
            // signing method is not in the listed set
            return Err(Error::TokenSignatureInvalid(format!(
                "signing method {} is invalid",
                token.method
            )));
        }

        // The signature is compared in constant time.
        if hmac_sha256(token.signing_string, key)
            .verify_slice(&token.signature)
            .is_err()
        {
            return Err(Error::TokenSignatureInvalid("signature is invalid".into()));
        }

        // Validate Claims
        self.validate(&token.claims, time_now())
            .map_err(Error::TokenInvalidClaims)?;

        Ok(token.claims)
    }

    // The library's Validator.Validate, for the checks that are asked for. It names all
    // that is wrong, not only the first.
    fn validate(&self, claims: &impl Claims, now: Time) -> Result<(), Vec<ClaimsError>> {
        let mut errs = Vec::new();

        // exp is good while now < exp + leeway.
        match claims.get_expiration_time() {
            None if self.expiration_required => {
                errs.push(ClaimsError::TokenRequiredClaimMissing("exp"));
            }
            None => {}
            Some(exp) if !before(now, exp, self.leeway) => errs.push(ClaimsError::TokenExpired),
            Some(_) => {}
        }

        // nbf is good from now >= nbf - leeway. A token need not have it.
        if let Some(nbf) = claims.get_not_before()
            && before(now, nbf, -self.leeway)
        {
            errs.push(ClaimsError::TokenNotValidYet);
        }

        // If we have an expected issuer, we also require the issuer claim
        if !self.issuer.is_empty() {
            let iss = claims.get_issuer();
            if iss.is_empty() {
                errs.push(ClaimsError::TokenRequiredClaimMissing("iss"));
            } else if iss != self.issuer {
                errs.push(ClaimsError::TokenInvalidIssuer);
            }
        }

        if errs.is_empty() { Ok(()) } else { Err(errs) }
    }
}

// The seconds from the year 1, where Go's `time.Time` starts to count, to 1970.
const UNIX_TO_INTERNAL: i64 = 62_135_596_800;

// Go's `now.Before(date.Add(d))`, with the arithmetic of Go's `time.Time`, which counts the
// seconds of a date from the year 1 in an int64. `time.Unix` adds the seconds up to 1970
// without looking, so a date more than 2^63 seconds after the year 1 comes out as one long
// before it; `Add` stops at the ends of what an int64 holds.
fn before(now: Time, date: NumericDate, d: Duration) -> bool {
    let date_sec = date
        .0
        .wrapping_add(UNIX_TO_INTERNAL)
        .saturating_add(d.nanoseconds().div_euclid(1_000_000_000));
    let date_nsec = d.nanoseconds().rem_euclid(1_000_000_000);
    let now_sec = now.unix_secs() + UNIX_TO_INTERNAL;
    (now_sec, now.nanosecond()) < (date_sec, date_nsec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::go::time;

    const KEY: &[u8] = b"a-key-for-these-tests";

    const PARSER: Parser<'static> = Parser {
        valid_methods: &[SIGNING_METHOD_HS256],
        issuer: "",
        leeway: time::MINUTE,
        expiration_required: true,
    };

    fn token(header: &str, claims: &str) -> String {
        let signing_string = format!(
            "{}.{}",
            encode_segment(header.as_bytes()),
            encode_segment(claims.as_bytes())
        );
        let signature = sign(&signing_string, KEY);
        format!("{signing_string}.{}", encode_segment(&signature))
    }

    fn claims(json: &str) -> Result<RegisteredClaims, Error> {
        parse_unverified::<RegisteredClaims>(&token(r#"{"alg":"HS256","typ":"JWT"}"#, json))
            .map(|token| token.claims)
    }

    // RFC 4231, test cases 1 and 2.
    #[test]
    fn hmac_sha256_of_the_rfc() {
        let hex = |bytes: Vec<u8>| -> String { bytes.iter().map(|b| format!("{b:02x}")).collect() };
        assert_eq!(
            hex(sign("Hi There", &[0x0b; 20])),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        assert_eq!(
            hex(sign("what do ya want for nothing?", b"Jefe")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    // RFC 7515, appendix A.1: the header and the claims of the example there, signed with
    // its key, give its signature.
    #[test]
    fn the_signature_of_the_example_in_rfc_7515() {
        let key = RAW_URL_ENCODING
            .decode(
                "AyM1SysPpbyDfgZld3umj1qzKObwVMkoqQ-EstJQLr_T-1qS0gZH75aKtMN3Yj0iPS4hcgUuTwjAzZr1Z9CAow",
            )
            .unwrap();
        let signing_string = "eyJ0eXAiOiJKV1QiLA0KICJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJqb2UiLA0KICJleHAiOjEzMDA4MTkzODAsDQogImh0dHA6Ly9leGFtcGxlLmNvbS9pc19yb290Ijp0cnVlfQ";
        assert_eq!(
            encode_segment(&sign(signing_string, &key)),
            "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
        );

        let token = format!("{signing_string}.dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk");
        let parsed = parse_unverified::<RegisteredClaims>(&token).unwrap();
        assert_eq!(parsed.claims.issuer, "joe");
        assert_eq!(parsed.claims.expires_at, Some(NumericDate(1300819380)));
        // It expired in 2011.
        let parser = Parser {
            expiration_required: false,
            ..PARSER
        };
        assert_eq!(
            parser.parse_with_claims::<RegisteredClaims>(&token, &key),
            Err(Error::TokenInvalidClaims(vec![ClaimsError::TokenExpired]))
        );
    }

    #[test]
    fn a_token_is_signed_and_read_back() {
        let claims = RegisteredClaims {
            issuer: "key".into(),
            subject: "someone".into(),
            expires_at: Some(NumericDate::new_after(time_now(), time::HOUR)),
            ..Default::default()
        };
        let token = signed_string(&claims, KEY).unwrap();
        assert!(token.starts_with("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."));
        assert_eq!(
            PARSER.parse_with_claims::<RegisteredClaims>(&token, KEY),
            Ok(claims)
        );
        assert_eq!(
            PARSER.parse_with_claims::<RegisteredClaims>(&token, b"another key"),
            Err(Error::TokenSignatureInvalid("signature is invalid".into()))
        );
    }

    #[test]
    fn only_what_is_set_is_written() {
        let json = |claims: &RegisteredClaims| serde_json::to_string(claims).unwrap();
        assert_eq!(json(&RegisteredClaims::default()), "{}");
        let all = RegisteredClaims {
            issuer: "i".into(),
            subject: "s".into(),
            audience: ClaimStrings(vec!["a".into()]),
            expires_at: Some(NumericDate(3)),
            not_before: Some(NumericDate(1)),
            issued_at: Some(NumericDate(2)),
            id: "j".into(),
        };
        assert_eq!(
            json(&all),
            r#"{"iss":"i","sub":"s","aud":["a"],"exp":3,"nbf":1,"iat":2,"jti":"j"}"#
        );
    }

    #[test]
    fn dates_are_whole_seconds() {
        let t = Time::unix(1700000000, 999_999_999);
        assert_eq!(NumericDate::new(t), NumericDate(1700000000));
        assert_eq!(NumericDate::new(t).time(), Time::unix(1700000000, 0));
        assert_eq!(NumericDate::new(Time::unix(-2, 500)), NumericDate(-2));
        assert_eq!(
            NumericDate::new_after(t, time::SECOND),
            NumericDate(1700000001)
        );
        // Three centuries from now is later than a Time can be, and still a date.
        let far = NumericDate::new_after(t, Duration::MAX);
        assert_eq!(far, NumericDate(1700000000 + 9223372036 + 1));
        assert_eq!(far.time(), Time::from_unix_nano(i64::MAX));

        let exp = |json: &str| claims(json).map(|claims| claims.expires_at.map(NumericDate::unix));
        assert_eq!(exp(r#"{"exp":1700000000}"#), Ok(Some(1700000000)));
        assert_eq!(exp(r#"{"exp":1700000000.75}"#), Ok(Some(1700000000)));
        assert_eq!(exp(r#"{"exp":-1.5}"#), Ok(Some(-2)));
        assert_eq!(exp(r#"{"exp":4.1e9}"#), Ok(Some(4100000000)));
        assert_eq!(exp(r#"{"exp":253402300799}"#), Ok(Some(253402300799)));
        assert_eq!(exp(r#"{"exp":1e-5}"#), Ok(Some(0)));
        assert_eq!(exp(r#"{"exp":-0}"#), Ok(Some(0)));
        // Every number is read as a float, also one that is written as an integer.
        assert_eq!(
            exp(r#"{"exp":9223371974719178753}"#),
            Ok(Some(9223371974719178752))
        );
        assert_eq!(exp(r#"{"exp":9.2e18}"#), Ok(Some(9200000000000000000)));
        // More seconds than 64 bits hold: the least number, as Go has it on amd64.
        assert_eq!(exp(r#"{"exp":9223372036854775807}"#), Ok(Some(i64::MIN)));
        assert_eq!(exp(r#"{"exp":18446744073709551615}"#), Ok(Some(i64::MIN)));
        assert_eq!(exp(r#"{"exp":1e30}"#), Ok(Some(i64::MIN)));
        assert_eq!(exp(r#"{"exp":"1e30"}"#), Ok(Some(i64::MIN)));
        assert_eq!(exp(r#"{"exp":-1e30}"#), Ok(Some(i64::MIN)));
        assert_eq!(exp(r#"{"exp":"1700000000"}"#), Ok(Some(1700000000)));
        assert_eq!(exp(r#"{"exp":"1.7e9"}"#), Ok(Some(1700000000)));
        assert_eq!(exp(r#"{"exp":null}"#), Ok(None));
        assert_eq!(exp(r#"{}"#), Ok(None));
        for bad in [
            r#"{"exp":"soon"}"#,
            r#"{"exp":""}"#,
            r#"{"exp":" 5"}"#,
            r#"{"exp":"5 "}"#,
            r#"{"exp":"+5"}"#,
            r#"{"exp":"0x5"}"#,
            r#"{"exp":"NaN"}"#,
            r#"{"exp":"1e400"}"#,
            r#"{"exp":true}"#,
            r#"{"exp":[5]}"#,
            r#"{"exp":{}}"#,
            r#"{"exp":1e400}"#,
        ] {
            assert!(
                matches!(exp(bad), Err(Error::TokenMalformed(_))),
                "{bad}: {:?}",
                exp(bad)
            );
        }
    }

    #[test]
    fn the_audience_is_a_string_or_a_list_of_strings() {
        let aud = |json: &str| claims(json).map(|claims| claims.audience.0);
        assert_eq!(aud(r#"{"aud":"x"}"#), Ok(vec!["x".to_string()]));
        assert_eq!(
            aud(r#"{"aud":["x","y"]}"#),
            Ok(vec!["x".to_string(), "y".to_string()])
        );
        assert_eq!(aud(r#"{"aud":null}"#), Ok(vec![]));
        assert_eq!(aud(r#"{"aud":[]}"#), Ok(vec![]));
        for bad in [r#"{"aud":5}"#, r#"{"aud":[5]}"#, r#"{"aud":["x",null]}"#] {
            assert_eq!(
                aud(bad),
                Err(Error::TokenMalformed(
                    "could not JSON decode claim: invalid type for claim".into()
                )),
                "{bad}"
            );
        }
    }

    #[test]
    fn the_times_of_a_token_are_checked_with_leeway() {
        let now = Time::unix(1700000000, 500_000_000);
        let check = |exp: Option<i64>, nbf: Option<i64>| {
            let claims = RegisteredClaims {
                expires_at: exp.map(NumericDate),
                not_before: nbf.map(NumericDate),
                ..Default::default()
            };
            PARSER.validate(&claims, now)
        };
        let expired = Err(vec![ClaimsError::TokenExpired]);
        let not_yet = Err(vec![ClaimsError::TokenNotValidYet]);

        assert_eq!(check(Some(1700003600), None), Ok(()));
        // A token is good until a minute after its expiry: while now < exp + leeway.
        assert_eq!(check(Some(1700000000 - 30), None), Ok(()));
        assert_eq!(check(Some(1700000000 - 59), None), Ok(()));
        assert_eq!(check(Some(1700000000 - 60), None), expired);
        assert_eq!(check(Some(1700000000 - 61), None), expired);
        assert_eq!(check(Some(0), None), expired);
        assert_eq!(check(Some(i64::MIN), None), expired);
        assert_eq!(check(Some(253402300799), None), Ok(()));
        // And from a minute before it starts: from now >= nbf - leeway.
        assert_eq!(check(Some(1700003600), Some(1700000000 - 5)), Ok(()));
        assert_eq!(check(Some(1700003600), Some(1700000000 + 30)), Ok(()));
        assert_eq!(check(Some(1700003600), Some(1700000000 + 60)), Ok(()));
        assert_eq!(check(Some(1700003600), Some(1700000000 + 61)), not_yet);
        assert_eq!(check(Some(1700003600), Some(i64::MIN)), Ok(()));
        // Go counts the seconds of a date from the year 1 in 64 bits. The last date that
        // holds is a date (and the leeway added to it does not go past the end); the one
        // after it is long ago. These are what Go's verifier says of the same dates.
        let last = i64::MAX - UNIX_TO_INTERNAL;
        assert_eq!(check(Some(9223371974719178752), None), Ok(()));
        assert_eq!(check(Some(last), None), Ok(()));
        assert_eq!(check(Some(last + 1), None), expired);
        assert_eq!(check(Some(9223371974719179776), None), expired);
        assert_eq!(check(Some(i64::MAX), None), expired);
        assert_eq!(check(Some(1700003600), Some(9223371974719178752)), not_yet);
        assert_eq!(check(Some(1700003600), Some(last)), not_yet);
        assert_eq!(check(Some(1700003600), Some(last + 1)), Ok(()));
        assert_eq!(check(Some(1700003600), Some(9223371974719179776)), Ok(()));
        assert_eq!(check(Some(1700003600), Some(i64::MAX)), Ok(()));
        // Everything that is wrong is named.
        assert_eq!(
            check(Some(1700000000 - 90), Some(1700000000 + 90)),
            Err(vec![
                ClaimsError::TokenExpired,
                ClaimsError::TokenNotValidYet
            ])
        );
        assert_eq!(
            Error::TokenInvalidClaims(vec![
                ClaimsError::TokenExpired,
                ClaimsError::TokenNotValidYet
            ])
            .to_string(),
            "token has invalid claims: token is expired, token is not valid yet"
        );

        // A token without exp is refused when exp is required, and only then.
        let missing = Err(vec![ClaimsError::TokenRequiredClaimMissing("exp")]);
        assert_eq!(check(None, None), missing);
        assert_eq!(
            Error::TokenInvalidClaims(vec![ClaimsError::TokenRequiredClaimMissing("exp")])
                .to_string(),
            "token has invalid claims: token is missing required claim: exp claim is required"
        );
        let lenient = Parser {
            expiration_required: false,
            ..PARSER
        };
        assert_eq!(lenient.validate(&RegisteredClaims::default(), now), Ok(()));
    }

    #[test]
    fn the_issuer_is_checked_when_one_is_expected() {
        let parser = Parser {
            issuer: "key",
            expiration_required: false,
            ..PARSER
        };
        let check = |issuer: &str| {
            let claims = RegisteredClaims {
                issuer: issuer.into(),
                ..Default::default()
            };
            parser.validate(&claims, time_now())
        };
        assert_eq!(check("key"), Ok(()));
        assert_eq!(check("other"), Err(vec![ClaimsError::TokenInvalidIssuer]));
        assert_eq!(
            check(""),
            Err(vec![ClaimsError::TokenRequiredClaimMissing("iss")])
        );
        // No issuer is expected: any will do.
        let any = Parser {
            issuer: "",
            ..parser
        };
        let claims = RegisteredClaims {
            issuer: "other".into(),
            ..Default::default()
        };
        assert_eq!(any.validate(&claims, time_now()), Ok(()));
    }

    #[test]
    fn the_header_says_how_the_token_is_signed() {
        let parse = |header: &str| {
            parse_unverified::<RegisteredClaims>(&token(header, "{}")).map(|token| token.method)
        };
        let unavailable = Err(Error::TokenUnverifiable(
            "signing method (alg) is unavailable".into(),
        ));
        let unspecified = Err(Error::TokenUnverifiable(
            "signing method (alg) is unspecified".into(),
        ));
        assert_eq!(parse(r#"{"alg":"HS256","typ":"JWT"}"#), Ok("HS256".into()));
        assert_eq!(parse(r#"{"alg":"HS256"}"#), Ok("HS256".into()));
        assert_eq!(
            parse(r#"{"alg":"HS256","typ":"at+jwt","kid":"k"}"#),
            Ok("HS256".into())
        );
        // Methods the library knows are refused only when the token is verified.
        for method in ["HS512", "RS256", "EdDSA", "none"] {
            let token = token(&format!(r#"{{"alg":"{method}","typ":"JWT"}}"#), "{}");
            assert_eq!(
                parse_unverified::<RegisteredClaims>(&token).map(|token| token.method),
                Ok(method.to_string())
            );
            assert_eq!(
                PARSER.parse_with_claims::<RegisteredClaims>(&token, KEY),
                Err(Error::TokenSignatureInvalid(format!(
                    "signing method {method} is invalid"
                )))
            );
        }
        assert_eq!(parse(r#"{"alg":"XX999"}"#), unavailable);
        assert_eq!(parse(r#"{"alg":"hs256"}"#), unavailable);
        assert_eq!(parse(r#"{"alg":5}"#), unspecified);
        assert_eq!(parse(r#"{"typ":"JWT"}"#), unspecified);
        assert_eq!(parse("null"), unspecified);
        assert!(matches!(parse("[]"), Err(Error::TokenMalformed(_))));
        assert!(matches!(parse("{"), Err(Error::TokenMalformed(_))));
    }

    #[test]
    fn a_token_is_three_segments_in_base64() {
        let exp = time_now().unix_secs() + 3600;
        let good = token(
            r#"{"alg":"HS256","typ":"JWT"}"#,
            &format!(r#"{{"exp":{exp}}}"#),
        );
        let verify = |token: &str| PARSER.parse_with_claims::<RegisteredClaims>(token, KEY);
        let invalid = Err(Error::TokenSignatureInvalid("signature is invalid".into()));
        let segments = Err(Error::TokenMalformed(
            "token contains an invalid number of segments".into(),
        ));
        assert!(verify(&good).is_ok());

        let parts: Vec<&str> = good.split('.').collect();
        assert_eq!(verify(&format!("{}.{}", parts[0], parts[1])), segments);
        assert_eq!(verify(&format!("{good}.x")), segments);
        assert_eq!(verify(""), segments);

        // Go's decoder skips line breaks, so one after the token does no harm. One before it
        // is in what was signed.
        assert!(verify(&format!("{good}\n")).is_ok());
        assert!(verify(&format!("{good}\r\n")).is_ok());
        assert_eq!(verify(&format!("\n{good}")), invalid);
        // Nothing else is skipped, and there is no padding.
        assert!(matches!(
            verify(&format!("{good} ")),
            Err(Error::TokenMalformed(_))
        ));
        assert!(matches!(
            verify(&format!("{good}=")),
            Err(Error::TokenMalformed(_))
        ));
        let other_alphabet = parts[2].replace('-', "+").replace('_', "/");
        if other_alphabet != parts[2] {
            assert!(matches!(
                verify(&format!("{}.{}.{other_alphabet}", parts[0], parts[1])),
                Err(Error::TokenMalformed(_))
            ));
        }

        // The signature.
        assert_eq!(verify(&format!("{}.{}.", parts[0], parts[1])), invalid);
        assert_eq!(
            verify(&format!("{}.{}.{}", parts[0], parts[1], &parts[2][..20])),
            invalid
        );
        // The last character of a signature of 32 bytes carries two bits that are not
        // used: Go does not look at them.
        let alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let last = alphabet.find(&parts[2][42..]).unwrap();
        let with_last = |index: usize| {
            format!(
                "{}.{}.{}{}",
                parts[0],
                parts[1],
                &parts[2][..42],
                &alphabet[index..index + 1]
            )
        };
        assert!(verify(&with_last(last ^ 1)).is_ok());
        assert_eq!(verify(&with_last(last ^ 4)), invalid);

        // The claims.
        let claims = |json: &str| verify(&token(r#"{"alg":"HS256","typ":"JWT"}"#, json));
        assert!(claims(&format!(r#" {{ "exp" : {exp} }} "#)).is_ok());
        assert!(matches!(
            claims(&format!(r#"{{"exp":{exp}}} x"#)),
            Err(Error::TokenMalformed(_))
        ));
        assert!(matches!(claims("[]"), Err(Error::TokenMalformed(_))));
        assert!(matches!(claims(r#""x""#), Err(Error::TokenMalformed(_))));
        // null leaves the claims empty: there is no exp then.
        assert_eq!(
            claims("null"),
            Err(Error::TokenInvalidClaims(vec![
                ClaimsError::TokenRequiredClaimMissing("exp")
            ]))
        );
    }
}
